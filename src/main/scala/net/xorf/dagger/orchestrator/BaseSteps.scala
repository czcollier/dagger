package net.xorf.dagger.orchestrator

import Model.{KV, StepResult}
import StandardKeys.PoisonPill
import net.xorf.dagger.Util.profile
import Model._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Built-in utility Steps for composing steps.
  */
object BaseSteps {

  def runStepWithTiming(step: Step, message: Message): StepResult = {
    val (result, time) = profile(step.processMessage(message))
    val stepTime = KV(s"${step.name}.Time", time)
    val fullTrace = stepTime :: result.trace
    StepResult(result.message, fullTrace)
  }

  /**
    * Step that wraps a set of steps and executes them in parallel
    * Requirements become the union of wrapped steps' requirements
    * Provisions become the union of wrapped steps' provisions
    * @param steps steps to execute in parallel
    * @param timeout timeout for Futures created for parallel execution
    */
  class ParallelStep(steps: Set[Step], timeout: FiniteDuration, lenient: Boolean = false) extends Step {

    import scala.concurrent.ExecutionContext.Implicits.global

    override val requires: Set[TypedKey[_]] = steps.flatMap(_.requires)
    override val produces: Set[TypedKey[_]] = steps.flatMap(_.produces)
    override val dependsOn: Set[Step] = Set[Step]()
    override val name = s"parallel [ ${steps.map(_.name).mkString(", ")} ]"

    override def processMessage(message: Message): StepResult = {
      val tasks: Seq[Future[StepResult]] = steps.toSeq.map { step =>
        Future {
          runStepWithTiming(step, message)
        }
      }

      val trys = Future.sequence(tasks.map(futureToFutureTry)).map(_.filter(_.isSuccess).map(_.get))
      trys.map { t =>
        t.reduce { (l, r) => StepResult(l.message ++ r.message, l.trace ::: r.trace) }
      }

      val reduced = if(lenient) reduceLenient(tasks) else reduceStrict(tasks)

      Await.result(reduced, timeout)
    }

    def reduceStrict(tasks: Seq[Future[StepResult]]): Future[StepResult] =
      Future.reduceLeft(tasks)(mergeStepResults)

    def reduceLenient(tasks: Seq[Future[StepResult]]): Future[StepResult] = {
      val futureSeq = Future.sequence(tasks.map(futureToFutureTry))
      val successes = futureSeq map { _.collect { case Success(x) => x }}
      val failures = futureSeq map { _.collect { case Failure(x) => x }}

      successes.map { t => t.reduce(mergeStepResults) }
    }

    def mergeStepResults(l: StepResult, r: StepResult): StepResult = {
      StepResult(l.message ++ r.message, l.trace ::: r.trace)
    }

    def futureToFutureTry[T](f: Future[T]): Future[Try[T]] =
      f.map(Success(_)).recover { case e => Failure(e) }
  }

  /**
    * Step that wraps a sequence of steps and executes them sequentially
    * Requirements become the requirements of the first step in the sequence
    * Provisions become the provisions of the last step in the sequence
    * @param steps steps to execute sequentially
    */
  class SequentialStep(steps: Seq[Step], val dependsOn: Set[Step] = Set()) extends Step {

    override val requires: Set[TypedKey[_]] = steps.head.requires
    override val produces: Set[TypedKey[_]] = steps.last.produces

    override val name = s"sequential [ ${steps.map(_.name).mkString(", ")} ]"

    override def processMessage(message: Message): StepResult = {
      val initialState = StepResult(message, List())

      steps.foldLeft(initialState) {
        (currentState, step) =>
          val timedResult = runStepWithTiming(step, currentState.message)

          timedResult.message.backingMap.keySet.collectFirst {
            case p: PoisonPill =>
              if (p.isLethal(timedResult.message))
                return StepResult(timedResult.message, currentState.trace ::: timedResult.trace)
          }

          StepResult(timedResult.message, currentState.trace ::: timedResult.trace)
      }
    }
  }
}

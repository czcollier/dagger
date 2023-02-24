package net.xorf.dagger.dsl

import net.xorf.dagger.orchestrator.BaseSteps.SequentialStep
import net.xorf.dagger.orchestrator.Model.{KV, StepResult, TypedKey}
import net.xorf.dagger.orchestrator.{Message, Step}

//suppresses compiler warnings about use of implicit conversions
import language.implicitConversions

object ConfigDSL {
  implicit def toTraced[T](value: T): Traced[T] = Traced(value, List())

  implicit def to2Tupled[I1, I2, Out](f: (I1, I2) => Out): ((I1, I2)) => Out = f.tupled
  implicit def to3Tupled[I1, I2, I3, Out](f: (I1, I2, I3) => Out): ((I1, I2, I3)) => Out = f.tupled

  case class Traced[T](data: T, trace: List[KV[_]])

  trait TypedStep[TIn, TOut] extends Step {
    def kout: TOut
  }

  implicit class StepInput[TKIn](kin: TKIn) {
    def ~>[TIn, TOut](fn: TIn => Traced[TOut])(implicit din: KeyTypeDerivation[TKIn, TIn]): StepWithInput[Nothing, TOut, TKIn] = {
      new StepWithInput[Nothing, TOut, TKIn](kin, fn)
    }
  }

  class StepWithInput[TIn, TOut, TKIn](kin: TKIn, fn: TIn => Traced[TOut]) {
    def ~>[TKOut](kout: TKOut) (implicit din: KeyTypeDerivation[TKIn, TIn], dout: KeyTypeDerivation[TKOut, TOut]) =
      new StepWithInputOutput(kin, kout, fn)
  }

  class StepWithInputOutput[TIn, TOut, TKIn, TKOut](kin: TKIn, kout: TKOut, fn: TIn => Traced[TOut]) {
    def ::(name: String)(implicit din: KeyTypeDerivation[TKIn, TIn], dout: KeyTypeDerivation[TKOut, TOut]): TypedStep[TKIn, TKOut] =
      step(name, kin, kout, fn)
  }

  private def step[TIn, TOut, TKIn, TKOut]
  (name: String, requires: TKIn, produces: TKOut, f: TIn => Traced[TOut])
    (implicit
     din: KeyTypeDerivation[TKIn, TIn],
     dout: KeyTypeDerivation[TKOut, TOut]
    ): TypedStep[TKIn, TKOut] = {

    val inKeys = din.derive(requires)
    val outKeys = dout.derive(produces)
    val req: Set[TypedKey[_]] = inKeys.keySet
    val prod: Set[TypedKey[_]] = outKeys.keySet
    val ko = produces
    val stepName = name
    val deps = Set[Step]()

    new TypedStep[TKIn, TKOut] {
      override val requires: Set[TypedKey[_]] = req
      override val produces: Set[TypedKey[_]] = prod
      override def dependsOn: Set[Step] = deps
      override val name: String = stepName
      override def kout: TKOut = ko
      override def processMessage(message: Message): StepResult = {
        val inVals: TIn = inKeys.get(message)
        val retVals = f(inVals)
        val outMsg = outKeys.put(message, retVals.data)
        StepResult(outMsg, retVals.trace)
      }
    }
  }

  def fstep[TIn, TOut, TKIn, TKOut]
  (name: String, dependsOnStep: TypedStep[_, TKIn], produces: TKOut, f: TIn => Traced[TOut])
    (implicit
      din: KeyTypeDerivation[TKIn, TIn],
      dout: KeyTypeDerivation[TKOut, TOut]
    ): TypedStep[TKIn, TKOut] = {

    step[TIn, TOut, TKIn, TKOut](name, dependsOnStep.kout, produces, f)
  }

  def sequential(
      name: String,
      steps: List[Step]): SequentialStep = {

    new SequentialStep(steps) {
      override val requires: Set[TypedKey[_]] =  steps.head.requires
      override val produces: Set[TypedKey[_]] = steps.flatMap(s => s.produces).toSet
    }
  }
}

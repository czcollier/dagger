package net.xorf.dagger.orchestrator

import BaseSteps.{ParallelStep, SequentialStep}
import Model.StepResult
import net.xorf.dagger.dag.DAG

trait Orchestrator {
  this: OrchestratorConfig =>

  implicit lazy val stepCanExecuteInDag: StepCanExecuteInDag = new StepCanExecuteInDag(stepConfig)

  lazy val sortedSteps: Seq[ParallelStep] = DAG.buildExecutionDAG(stepConfig)
    .map(s => new ParallelStep(s, futureTimeout))

  def processMessage(message: Message): StepResult =
    new SequentialStep(sortedSteps).processMessage(message)

}

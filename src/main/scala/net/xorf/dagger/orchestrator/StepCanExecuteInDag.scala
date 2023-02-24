package net.xorf.dagger.orchestrator

import StandardKeys.PoisonPill
import net.xorf.dagger.dag.CanExecuteInDAG

class StepCanExecuteInDag(stepLibrary: Iterable[Step]) extends CanExecuteInDAG[Step] {
  private def isDependent(left: Step, right: Step) =
    left.produces.intersect(right.requires).nonEmpty

  override def dependencies(step: Step): Seq[Step] = {
    val keyDeps = stepLibrary.filter(s => isDependent(s, step))
    step.dependsOn.toSeq ++ keyDeps
  }

  override def haltsExecution(step: Step): Boolean =
    dependencies(step).exists(_.isInstanceOf[PoisonPill])
}

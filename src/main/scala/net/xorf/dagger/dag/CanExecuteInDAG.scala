package net.xorf.dagger.dag

trait CanExecuteInDAG[A] {
  def dependencies(a: A): Seq[A]
  def haltsExecution(a: A): Boolean
}

object CanExecuteInDAG {
  def dependencies[A: CanExecuteInDAG](a: A): Seq[A] = CanExecuteInDAG[A].dependencies(a)
  def haltsExecution[A: CanExecuteInDAG](a: A): Boolean = CanExecuteInDAG[A].haltsExecution(a)

  def apply[A](implicit hd: CanExecuteInDAG[A]): CanExecuteInDAG[A] = hd

  implicit class HasDependenciesOps[A](val a: A) extends AnyVal {
    def dependencies(implicit hd: CanExecuteInDAG[A]): Seq[A] = hd.dependencies(a)
    def haltsExecution(implicit hd: CanExecuteInDAG[A]): Boolean = hd.haltsExecution(a)
  }
}

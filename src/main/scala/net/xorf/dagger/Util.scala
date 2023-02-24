package net.xorf.dagger

object Util {
  /**
    * For micro-profiling code.
    *
    * @param code
    * @param startTime
    * @tparam T
    * @return
    */
  def profile[T](code: => T, startTime: Long = System.nanoTime): (T, Long) =
    (code, System.nanoTime - startTime)
}

package net.xorf.dagger.orchestrator

object Model {
  trait TypedKey[T]
  /**
    * Output of a step.  Contains a message and list of key-value
    * pairs called a trace.  The trace is for outputting diagnostic
    * information
    */
  case class StepResult(message: Message, trace: List[KV[_]])


  /**
    * Key value pair
    */
  case class KV[T](key: String, value: T)
}

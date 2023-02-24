package net.xorf.dagger.orchestrator

import Model.TypedKey

object StandardKeys {

  trait PoisonPill {
    def isLethal(message: Message): Boolean
  }

  trait HaltIfTrue extends PoisonPill with TypedKey[Boolean] {
    override def isLethal(message: Message): Boolean = message.get(this).exists(x => x)
  }

  trait HaltIfFalse extends PoisonPill with TypedKey[Boolean] {
    override def isLethal(message: Message): Boolean = message.get(this).exists(x => !x)
  }

  trait HaltIfNone[T] extends PoisonPill with TypedKey[Option[T]] {
    override def isLethal(message: Message): Boolean = message.get(this).exists(x => x.isDefined)
  }
}

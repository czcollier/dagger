package net.xorf.dagger.orchestrator

import Model.TypedKey
import net.xorf.dagger.orchestrator.Model.TypedKey

/**
  * Message object that flows through steps in orchestrator chain
  *
  * This is basically a typed map -- keys are individually typed.
  */
class Message(private val back: Map[TypedKey[_], _]) extends Iterable[(TypedKey[_], _)] {
  def this() = this(Map())

  def get[T](key: TypedKey[T]): Option[T] =
    back.get(key).map(_.asInstanceOf[T])

  def select(keys: Seq[MessageKey[_]]): Message =
    new Message(back.filterKeys(k => keys.toSet.contains(k)).toMap)

  /** returns a new Message containing all elements plus new entry (key -> value) */
  def +[T](kv: (MessageKey[T], T)) = new Message(back + kv)

  /** returns a new message with all elements of both this and msg */
  def ++(msg: Message) = new Message(back ++ msg.back)

  /** returns a new message with key removed */
  def -[T](key: MessageKey[T]) = new Message(back - key)

  def contains[T](key: MessageKey[T]): Boolean = back.contains(key)

  def apply[T](key: MessageKey[T]): T = back(key).asInstanceOf[T]

  override def toString: String = {
    val elemsStr = back.map((kv: (TypedKey[_], _)) => s"${kv._1} -> ${kv._2}").mkString(", ")
    s"Message($elemsStr)"
  }

  def backingMap: Map[TypedKey[_], _] = Map() ++ back

  override def iterator = backingMap.iterator
}

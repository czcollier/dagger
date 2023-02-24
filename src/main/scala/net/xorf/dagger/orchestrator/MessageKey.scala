package net.xorf.dagger.orchestrator

import Model.TypedKey

import scala.reflect.ClassTag

/** typed key that goes in a message */
class MessageKey[T](val name: String)(implicit tt: ClassTag[T]) extends TypedKey[T] {
  override def toString: String = {
    s"MessageKey[${tt.runtimeClass.getSimpleName}]($name)"
  }
}

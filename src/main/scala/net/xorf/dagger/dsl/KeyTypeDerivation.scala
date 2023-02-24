package net.xorf.dagger.dsl

import KeyTypeDerivation.GenericMessageKey
import net.xorf.dagger.orchestrator.{Message, MessageKey}
import net.xorf.dagger.orchestrator.Model.TypedKey
import shapeless._

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Key list of type: ${KT} does not match function parameters: ${V}. Please check that the key lists match the step function signature.")
trait KeyTypeDerivation[KT, V] {
  def derive(kt: KT): GenericMessageKey[V]
}

object KeyTypeDerivation {

  trait GenericMessageKey[T] extends TypedKey[T] {
    def get(message: Message): T
    def put(message: Message, v: T): Message
    def keySet: Set[TypedKey[_]]
  }

  /**
    * Base case -- derive a GenericMessageKey the simply yields HNil given HNil
    *
    * @return
    */
  implicit def deriveHNil: KeyTypeDerivation[HNil, HNil] = new KeyTypeDerivation[HNil, HNil] {
    override def derive(k: HNil): GenericMessageKey[HNil] = new GenericMessageKey[HNil] {
    override def get (message: Message) = HNil
    override def put (message: Message, v: HNil) = message
    override def keySet = Set ()
  }
  }

  implicit def deriveSingular[V, KT <: MessageKey[V]]: KeyTypeDerivation[KT, V] = {
    (kt: KT) => new GenericMessageKey[V] {
      override def get(message: Message) = message(kt)
      override def put(message: Message, v: V) = message + (kt -> v)
      override def keySet = Set(kt)
    }
  }

  /**
    * Inductive definition of KeyTypeDerivation.  Given types K and V, and an implicit
    * KeyTypeDerivation can be discovered that resolves KRest and DerivRest, build a
    * KeyTypeDerivation that resolves KT :: KRest and V :: VRest
    * @param ktd implicit KeyTypeDerivation[KRest, DerivRest]
    * @tparam V Value type for the key (the key's type parameter type)
    * @tparam K Key type
    * @tparam KRest GenericMessageKey type for implicit derivation
    * @tparam VRest Value type for implicit derivation
    * @return
    */
  implicit def deriveHCons[V,  K <: MessageKey[V], KRest <: HList, VRest <: HList]
  (implicit ktd: KeyTypeDerivation[KRest, VRest]): KeyTypeDerivation[K :: KRest, V :: VRest] =
    (kt: K :: KRest) => {
      new GenericMessageKey[V :: VRest] {
        private lazy val parent = ktd.derive(kt.tail)

        override def get(message: Message): V :: VRest = {
          message(kt.head) :: parent.get(message)
        }

        override def put(message: Message, v: ::[V, VRest]): Message = {
          message + (kt.head -> v.head) ++ parent.put(message, v.tail)
        }

        override def keySet = Set(kt.head) ++ parent.keySet
      }
    }

  /**
    * Derive generic case.  This will allow implicit derivation of GenericMessageKey for
    * Product types, such at Tuples and case classes.
    * @param kgen Generic.Aux that can build a key from the generic type
    * @param gen Generic.Aux that can build the value from the gneric type
    * @param rd KeyTypeDerivation for the Product type
    * @tparam T value type
    * @tparam K key type
    * @tparam KGen Generic key type
    * @tparam VGen Generic value type
    * @return
    */
  implicit def deriveGeneric[T, K, KGen <: HList, VGen <: HList]
  (implicit kgen: Generic.Aux[K, KGen], gen: Generic.Aux[T, VGen], rd: KeyTypeDerivation[KGen, VGen]): KeyTypeDerivation[K, T] =
    (l: K) => new GenericMessageKey[T] {
      private lazy val parent = rd.derive(kgen.to(l))
      override def get(message: Message) = gen.from(parent.get(message))
      override def put(message: Message, v: T): Message = parent.put(message, gen.to(v))
      override def keySet = parent.keySet
    }
}

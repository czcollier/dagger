package net.xorf.dagger.orchestrator

import Model.{StepResult, TypedKey}

/**
  * A Step that can be scheduled and executed by the orchestrator
  */
trait Step {
  def requires: Set[TypedKey[_ <: Any]]
  def produces: Set[TypedKey[_ <: Any]]
  def dependsOn: Set[Step]
  def name: String
  def processMessage(message: Message): StepResult
  override def toString = name
}



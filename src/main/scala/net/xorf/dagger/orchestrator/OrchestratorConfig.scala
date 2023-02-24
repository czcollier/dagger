package net.xorf.dagger.orchestrator

import scala.concurrent.duration.FiniteDuration

trait OrchestratorConfig {
  val stepConfig: Seq[Step]
  val futureTimeout: FiniteDuration
}

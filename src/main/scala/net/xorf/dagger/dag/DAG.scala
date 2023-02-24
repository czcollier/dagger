package net.xorf.dagger.dag

import scala.annotation.tailrec
import scala.language.postfixOps

/*
TODO:

For future optimization of the DAG, we determined that we wanted to be able to stop evaluation
sooner when a validation fails.  To this end, we determined that there should be a new kind of
node, a Validator.  Validators effectively wrap any step or group of parallel steps where the
output of those steps can stop computation of the graph.  E.g. A filter that only allows
messages with a certain property through.  If that step fails, no other steps should be run.

In order to use this concept, after the DAG is created and the topological sort occurs, the
DAG should be re-arranged according to the following rules/constraints:

Constraints:
- No action in the re-arranging may break an outgoing edge (direction reflects the direction of
  data flow)
- No action may create a cycle
- Actions consist of adding incoming edges

Goals:
- Combine sequential validators to be parallelized
  - When a parallel execution includes both validators and non-validators, the node should be split
    s.t. the validators all run first.
- Try to move all Validators as far rootward as possible (without violating any of the constraints)
  - Nodes should only attempt to become the child of the most-rootward Validator that outputs the keys they need
 */


/**
  * Utilities for building execution schedules for steps
  * based on dependencies between them.
  */
object DAG {

  /**
    * Build a parallel schedule for a graph using topological sort.
    *
    * Kahn's algorithm
    * https://en.wikipedia.org/wiki/Topological_sorting#Kahn.27s_algorithm
    * Heavily inspired by code found here:
    * https://gist.github.com/ThiporKong/4399695
    *
    * @param edges Adjacency list -- Map from vertex -> Set(vertex)
    * @tparam A vertex type
    * @return sorted vertices
    */
  def buildParallelSchedule[A](edges: Map[A, Set[A]]): Seq[Set[A]] = {
      @tailrec
      def topologicalSort(graph: Map[A, Set[A]], done: List[Set[A]]): Seq[Set[A]] = {
          val (noDeps, haveDeps) = graph.partition { _._2.isEmpty }
          if (noDeps.isEmpty) {
              if (haveDeps.isEmpty) done
              else throw new IllegalStateException("cyclic reference in: " + haveDeps.toString)
          } else {
              val found = noDeps.keySet
              topologicalSort(haveDeps.mapValues(_.--(found)).toMap, done ::: List(found))
          }
      }
      topologicalSort(edges, List())
  }

  import CanExecuteInDAG._
  /**
    * Build a dependency based execution schedule (DAG) for a sequence of steps
    *
    * @param steps
    * @return Sequence of sets of steps.
    */
  def buildExecutionDAG[A](steps: Seq[A])(implicit hd: CanExecuteInDAG[A]): Seq[Set[A]] = {
    val graph = steps.map(s => (s, s.dependencies.toSet)).toMap

    val preliminarySchedule = buildParallelSchedule(graph)

    val validatedSchedule = preliminarySchedule.flatMap { stage =>
      val (validators, processors) = stage.partition(_.haltsExecution)
      if (validators.nonEmpty) {
        if (processors.isEmpty)
          Seq(validators)
        else
          Seq(validators, processors)
      }
      else Seq(stage)
    }

    validatedSchedule
  }
}

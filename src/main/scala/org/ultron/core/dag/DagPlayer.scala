package org.ultron.core.dag

import java.util.concurrent.TimeUnit
import akka.actor.{Actor, ActorRef}
import org.ultron.config.AppContext
import org.ultron.core.AppLogger
import org.ultron.core.dag.Message._
import scala.concurrent.duration._


/**
 * Created by chlr on 1/7/16.
 */

class DagPlayer(dag: Dag, app_context: AppContext, val router: ActorRef) extends Actor {

  def play: Receive = {
    case 'Play => {
      implicit val dispatcher = context.dispatcher
      val heartbeat_interval = Duration(app_context.dagSetting.heartbeat_cycle.toMillis, TimeUnit.MILLISECONDS)
      AppLogger debug s"scheduling heartbeat messages for $heartbeat_interval"
      context.system.scheduler.schedule(0 seconds, heartbeat_interval, self, new Tick)
    }
  }

  override def receive: Receive = healthyDag orElse onTaskComplete orElse play

  def healthyDag: Receive = {
    case tick: Tick => {
      if (dag.hasCompleted) {
        AppLogger info "all tasks completed. System shutting down"
        context.system.shutdown()
        sys.exit(0)
      }
      else {
        dag.getRunnableNodes foreach {
          node => {
            AppLogger info s"launching task ${node.name}"
            router ! TaskWrapper(node.name,node.getNodeTask(app_context))
            node.setStatus(Status.RUNNING)
          }
        }
      }
    }
  }

  def woundedDag: Receive = {
    case tick: Tick => {
      if (dag.getNodesWithStatus(Status.RUNNING).nonEmpty) {
          val failed_nodes = dag.getNodesWithStatus(Status.FAILED)
          val running_nodes = dag.getNodesWithStatus(Status.RUNNING)
          AppLogger info s"$failed_nodes failed. waiting for $running_nodes to complete"
      }
      else {
        AppLogger info "all nodes completed. System shutting down"
        context.system.shutdown()
        sys.exit(-1)
      }
    }
  }

  def onTaskComplete: Receive = {

    case message: TaskSuceeded => {
      AppLogger info s"task ${message.name} completed successfully. commiting checkpoint"
      checkpoint(message.name,message.task_stats)
      AppLogger info s"checkpoint completed successfully for ${message.name}"
    }

    case message: TaskFailed => {
      AppLogger info s"task ${message.name} execution failed. commiting checkpoint"

      checkpoint(message.name,message.task_stats)
      context.become(woundedDag orElse onTaskComplete)
      AppLogger info s"checkpoint completed successfully for ${message.name}"
    }

  }

  def checkpoint(name: String, task_stats: TaskStats) = {

    AppLogger debug s"running checkpoint for $name"
    dag.getNodeByName(name).setStatus(task_stats.status)
    app_context.writeCheckpoint(name,task_stats)
    dag.updateNodePayloads(app_context.payload)
  }

}




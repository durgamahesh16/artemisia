package tech.artemisia.core.dag

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import tech.artemisia.config.AppContext
import tech.artemisia.core.dag.Message.{TaskFailed, TaskStats, TaskSuceeded, Tick, _}
import tech.artemisia.core.{AppLogger, Keywords}

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

  override def receive: Receive = preReceive andThen (healthyDag orElse onTaskComplete orElse play)

  def healthyDag: Receive = {
    case tick: Tick => {
      if (dag.hasCompleted) {
        AppLogger info "all tasks completed. System shutting down"
        context.system.shutdown()
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

  def preReceive: PartialFunction[Any,Any] = {
    case x: Any => Thread.currentThread().setName(Keywords.APP); x
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
      AppLogger info s"task ${message.name} completed successfully. committing checkpoint"
      checkpoint(message.name,message.task_stats)
      AppLogger info s"checkpoint completed successfully for ${message.name}"
    }

    case message: TaskFailed => {
      AppLogger info s"task ${message.name} execution failed. committing checkpoint"
      checkpoint(message.name,message.taskStats)
      AppLogger info s"checkpoint completed successfully for ${message.name}"
      message.taskStats.status match {
        case Status.FAILED => {
          context.become(preReceive andThen (woundedDag orElse onTaskComplete))
        }
        case Status.FAILURE_IGNORED => {
          AppLogger info s"task ${Keywords.Task.IGNORE_ERROR} property set to true. Ignoring error"
        }
      }
    }

  }

  def checkpoint(name: String, task_stats: TaskStats) = {

    AppLogger debug s"running checkpoint for $name"
    dag.getNodeByName(name).setStatus(task_stats.status)
    app_context.writeCheckpoint(name,task_stats)
    dag.updateNodePayloads(app_context.payload)
  }

}




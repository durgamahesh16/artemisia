package tech.artemisia.dag

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.Keywords.Task
import tech.artemisia.core.{AppContext, AppLogger, Keywords}
import tech.artemisia.task.{TaskConfig, TaskHandler}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.collection.LinearSeq

/**
 * Created by chlr on 1/4/16.
 */

class Node(val name: String, var payload: Config) {

  /* this variable is expected to be handled by actors
     access outside of the actor system might coroupt the state of the node
  */

  private var status = Status.READY
  val ignoreFailure: Boolean = false
  var parents: LinearSeq[Node] = Nil

  def isRunnable = {
    (parents forall { _.isComplete }) && this.status == Status.READY // forall for Nil returns true
  }

  def isComplete = {
    Seq(Status.SUCCEEDED, Status.SKIPPED, Status.FAILURE_IGNORED) contains status
  }

  def getNodeTask(app_context: AppContext): TaskHandler = {
    val component = app_context.componentMapper(payload.as[String](Task.COMPONENT))
    val task = component.dispatch(payload.as[String](Keywords.Task.TASK), name, payload.as[Config](Keywords.Task.PARAMS))
    new TaskHandler(TaskConfig(name,payload,app_context),task)
  }

  override def equals(that: Any): Boolean = {
    that match {
      case that: Node => that.name == this.name
      case _ => false
    }
  }


  def setStatus(status: Status.Value): Unit = {
    AppLogger info s"node $name status set to $status"
    this.status = status
  }

  def getStatus = status

  def applyStatusFromCheckpoint(checkpointStatus: Status.Value): Unit = {

    checkpointStatus match {
      case Status.SUCCEEDED => {
        AppLogger info s"marking node $name status as $checkpointStatus from checkpoint"
        this.status = checkpointStatus
      }
      case Status.SKIPPED => {
        AppLogger info s"marking node $name status as $checkpointStatus from checkpoint"
        this.status = checkpointStatus
      }
      case Status.FAILED => {
        AppLogger info s"detected node $name from checkpoint has ${Status.FAILED}. setting node status as ${Status.READY}"
      }
      case Status.FAILURE_IGNORED => {
        AppLogger info s"marking node $name status as $checkpointStatus from checkpoint"
    }
      case _ => {
        AppLogger warn s"node $name has an unhandled status of $checkpointStatus}. setting node status as ${Status.READY}"
      }
    }
  }

  override def toString = {
    s"$name"
  }

}

object Node {

  def apply(name: String, body: Config) = {
    new Node(name,body)
  }

  def apply(name: String) = {
    new Node(name,ConfigFactory.empty())
  }

  def unapply(node: Node) = {
    Some(node.name, node.parents)
  }

}

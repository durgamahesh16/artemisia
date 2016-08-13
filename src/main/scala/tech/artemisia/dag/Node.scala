package tech.artemisia.dag

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import tech.artemisia.core.Keywords.Task
import tech.artemisia.core.{AppContext, AppLogger, Keywords}
import tech.artemisia.task.{TaskConfig, TaskHandler}
import tech.artemisia.util.HoconConfigUtil.{Handler, configToConfigEnhancer}

import scala.collection.LinearSeq

/**
  * Created by chlr on 1/4/16.
  */

class Node(val name: String, var payload: Config) {

  def resolvedPayload(code: Config) = {
    // we do this so that assertions are resolved now and only after task execution completes
    val assertions = payload.getAs[ConfigValue](Keywords.Task.ASSERTION)
    val variables = payload.getAs[Config](Keywords.Task.VARIABLES)
      .getOrElse(ConfigFactory.empty())
    val config = payload
      .withoutPath(Keywords.Task.ASSERTION)
      .withoutPath(Keywords.Task.VARIABLES)
      .withFallback(variables)
      .withFallback(code)
      .hardResolve
    assertions match {
      case Some(x) => config.withValue(Keywords.Task.ASSERTION, x)
      case None => config
    }
  }

  private var status = Status.READY
  val ignoreFailure: Boolean = false
  var parents: LinearSeq[Node] = Nil

  def isRunnable = {
    (parents forall {
      _.isComplete
    }) && this.status == Status.READY // forall for Nil returns true
  }

  def isComplete = {
    Seq(Status.SUCCEEDED, Status.SKIPPED, Status.FAILURE_IGNORED) contains status
  }

  def getNodeTask(app_context: AppContext): TaskHandler = {
    val config = resolvedPayload(app_context.payload)
    val componentName = config.as[String](Task.COMPONENT)
    val taskName = config.as[String](Keywords.Task.TASK)
    val defaults = app_context.payload.getAs[Config](s""""${Keywords.Config.DEFAULTS}"."$componentName"."$taskName"""")
    val component = app_context.componentMapper(componentName)
    val task = component.dispatchTask(taskName, name, config.as[Config](Keywords.Task.PARAMS) withFallback
      defaults.getOrElse(ConfigFactory.empty()))
    new TaskHandler(TaskConfig(config, app_context), task)
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
    new Node(name, body)
  }

  def apply(name: String) = {
    new Node(name, ConfigFactory.empty())
  }

  def unapply(node: Node) = {
    Some(node.name, node.parents)
  }

}

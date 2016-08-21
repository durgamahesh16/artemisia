package tech.artemisia.dag

import com.typesafe.config._
import tech.artemisia.core.Keywords.Task
import tech.artemisia.core.{AppContext, AppLogger, Keywords}
import tech.artemisia.task.{TaskConfig, TaskHandler}
import tech.artemisia.util.HoconConfigUtil.{Handler, configToConfigEnhancer}

import scala.collection.JavaConverters._

/**
  * Created by chlr on 1/4/16.
  */

final class Node(val name: String, var payload: Config) {


  private var status = Status.READY

  val ignoreFailure: Boolean = false

  private var _parents: Seq[Node] = Nil

  /**
    * getter for parents attribute
    * @return
    */
  def parents = _parents

  /**
    * setter for parents attribute
    * @param nodes new parents for the
    */
  def parents_=(nodes: Seq[Node]) = {
    payload =  ConfigFactory.empty().withValue(Keywords.Task.DEPENDENCY,
      ConfigValueFactory.fromIterable(nodes.map(_.name).asJava)) withFallback payload
    _parents = nodes
  }


  /**
    * config payload used for resolving the node payload.
    * This is the entire job config excluding special nodes
    * for worklets, connections, settings and defaults.
    * @param contextConfig job config
    * @return context config which is used for resolving variables.
    */
  private def getcontextConfig(contextConfig: Config) = {
    val variables = payload.getAs[Config](Keywords.Task.VARIABLES)
        .getOrElse(ConfigFactory.empty())
    variables withFallback
    contextConfig
      .withoutPath(Keywords.Config.WORKLET)
      .withoutPath(Keywords.Config.DEFAULTS)
      .withoutPath(Keywords.Config.SETTINGS_SECTION)
      .withoutPath(Keywords.Config.CONNECTION_SECTION)
  }


  /**
    * generate resolved payload for the node.
    * The entire node payload is resolved except the assertion field of the node.
    * @param contextConfig contextConfig for this node.
    * @return resolved payload of the node.
    */
  def resolvedPayload(contextConfig: Config) = {
    // we do this so that assertions are not resolved now and only after task execution completes
    val assertions = payload.getAs[ConfigValue](Keywords.Task.ASSERTION)
    val variables = payload.getAs[Config](Keywords.Task.VARIABLES)
      .getOrElse(ConfigFactory.empty())
    // for predictably and to generate clean config
    // we exclude any special nodes like __worklet__, __defaults__ during resolution.
    val config = payload
      .withoutPath(Keywords.Task.ASSERTION)
      .withoutPath(Keywords.Task.VARIABLES)
      .hardResolve(contextConfig)
    assertions match {
      case Some(x) => config.withValue(Keywords.Task.ASSERTION, x)
      case None => config
    }
  }


  /**
    *
    * @return true if the node is in runnable state.
    */
  def isRunnable = {
    (_parents forall {
      _.isComplete
    }) && this.status == Status.READY // forall for Nil returns true
  }

  /**
    *
    * @return
    */
  def isComplete = {
    Seq(Status.SUCCEEDED, Status.SKIPPED, Status.FAILURE_IGNORED) contains status
  }

  /**
    *
    * @param jobPayload
    * @param appContext
    * @return
    */
  def getNodeTask(jobPayload: Config, appContext: AppContext): TaskHandler = {
    val contextConfig = getcontextConfig(jobPayload)
    val config = resolvedPayload(contextConfig)
    val componentName = config.as[String](Task.COMPONENT)
    val taskName = config.as[String](Keywords.Task.TASK)
    val defaults = appContext.payload.getAs[Config](s""""${Keywords.Config.DEFAULTS}"."$componentName"."$taskName"""")
    val component = appContext.componentMapper(componentName)
    val task = component.dispatchTask(taskName, name, config.as[Config](Keywords.Task.PARAMS) withFallback
      defaults.getOrElse(ConfigFactory.empty()))
    new TaskHandler(TaskConfig(config, appContext), task, contextConfig)
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

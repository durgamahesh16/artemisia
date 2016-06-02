package tech.artemisia.dag

import com.typesafe.config._
import tech.artemisia.core.BasicCheckpointManager.CheckpointData
import tech.artemisia.core.Keywords.Task
import tech.artemisia.core._
import tech.artemisia.dag.Message.TaskStats
import tech.artemisia.task.TaskContext
import tech.artemisia.util.HoconConfigUtil.{Handler, configToConfigEnhancer}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.LinearSeq

/**
 * Created by chlr on 1/3/16.
 */

private[dag] class Dag(node_list: LinearSeq[Node], checkpointData: CheckpointData) extends Iterable[LinearSeq[Node]] {

  this.resolveDependencies(node_list)
  AppLogger debug "resolved all task dependency"
  val graph = topSort(node_list)
  AppLogger debug "no cyclic dependency detected"
  this.applyCheckpoints(checkpointData)

  @tailrec
  private def topSort(unsorted_graph: LinearSeq[Node], sorted_graph: LinearSeq[Node] = Nil):LinearSeq[Node] = {
    (unsorted_graph ,sorted_graph) match {
      case (Nil,a) =>  a
      case _ => {
        val open_nodes = unsorted_graph collect {
          case node @ Node(_,Nil) => node
          case node @ Node(_, parents) if parents forall { sorted_graph contains _ } => node
        }
        if (open_nodes isEmpty) {
          AppLogger error { s"cyclic dependency detected in graph structure $unsorted_graph" }
          throw new DagException("Cycles Detected in Dag")
        }
        topSort(unsorted_graph filterNot { open_nodes contains _  },sorted_graph ++ open_nodes)
      }
    }
  }

  protected[dag] def resolveDependencies(nodeList: LinearSeq[Node]): Unit = {
    val nodeMap = (nodeList map { x => { x.name -> x } } ).toMap
    nodeList map { x => x -> x.payload.getAs[List[String]](Keywords.Task.DEPENDENCY) } filter {
      x => x._2.nonEmpty
    } foreach {
      case (node,dependency) => {
        node.parents = dependency.get map {
          x => {
            if ((nodeMap get x).isEmpty) {
              AppLogger error s"invalid dependency reference for $x in ${node.name}"
              throw new DagException(s"invalid dependency reference for $x in ${node.name}")
            }
            nodeMap(x)
          }
        }
      }
    }
  }

  def updateNodePayloads(code: Config) = {
    Dag.parseNodeFromConfig(code) foreach {
      case (node_name,payload) => this.getNodeByName(node_name).payload = payload
    }
  }

  private def applyCheckpoints(checkpointData: CheckpointData): Unit = {
    AppLogger info "applying checkpoints"
    checkpointData.taskStatRepo foreach {
        case (task_name,task_stats: TaskStats) => {
          val node = this.getNodeByName(task_name)
          node.applyStatusFromCheckpoint(task_stats.status)
      }
    }
  }

  def getNodeByName(name: String) = {
    val node = graph filter { _.name == name }
    assert(node.size == 1, s" A single node by the name $name must exist")
    node.head
  }

  def hasCompleted = {
    graph forall { _.isComplete }
  }

  def getRunnableNodes: LinearSeq[Node] = {
    graph filter { _.isRunnable }
  }


  def getNodesWithStatus(status : Status.Value) = {
    graph filter {_.getStatus == status}
  }

  override def toString() = graph.toString()

  override def iterator: Iterator[LinearSeq[Node]] = new Iterator[LinearSeq[Node]] {

    var traversed = Set[Node]()
    val source = graph.toSet[Node]

    override def hasNext: Boolean = (source diff traversed).nonEmpty

    override def next(): LinearSeq[Node] = {
      val open_nodes = (source diff traversed) filter { x => (x.parents.toSet[Node] diff traversed).isEmpty }
      traversed = traversed ++ open_nodes
      open_nodes.toList
    }
  }
}


object Dag {

  def apply(appContext: AppContext) = {
   val node_list = parseNodeFromConfig(appContext.checkpoints.adhocPayload withFallback  appContext.payload) map {
     case (name,payload) => Node(name,payload)
   }
   new Dag(node_list.toList,appContext.checkpoints)
  }

  def apply(node_list: LinearSeq[Node], checkpointData: CheckpointData = CheckpointData()) = {
    new Dag(node_list,checkpointData)
  }

  def parseNodeFromConfig(code: Config): Map[String, Config] = {
    val assertNodes = extractTaskAssertionNodes(code)
    val resolvedConfig = code.hardResolve
    // this is done to make assert nodes un-resolved.
    TaskContext.payload  = assertNodes.foldLeft(resolvedConfig) {
      (tempConfig, kv) => {
        tempConfig.withValue(s"${kv._1}.${Keywords.Task.ASSERTION}", kv._2) withFallback tempConfig
      }
    }
      extractTaskNodes(TaskContext.payload) map {
        case (name, body: ConfigObject) => name -> body.toConfig
      }
  }


  /**
    * This method parses a given Config Object and identifies task definition nodes and filters rest.
    *
    * for eg: consider below node the result will be Map("task" -> SimpleConfigObject())
    * here the node foo = bar was filter and task and its corresponding configobject value is selected.
    *
    * {{{
    *   foo = bar
    *   task = {
    *     Component = MyComponent
    *     Task = MyTask
    *     param = {
    *
    *     }
    *   }
    * }}}
    *
    * @param config config payload to be parsed
    * @return Map of taskname and task definition config objects
    */
  private[dag] def extractTaskNodes(config: Config): Map[String, ConfigValue] = {
    config.root() filterNot {
      case (key, value) => key.startsWith("__") && key.endsWith("__")
    } filter {
      case (key, value)  =>
        value.valueType() == ConfigValueType.OBJECT
      case _ => false
    } filter {
      case (key, value: ConfigObject) =>
        value.toConfig.hasPath(Task.COMPONENT) && value.toConfig.hasPath(Keywords.Task.TASK)
    } toMap
  }

  def extractTaskAssertionNodes(config: Config) = {
    extractTaskNodes(config) filter {
      case (taskName, taskDef: ConfigObject) => taskDef.contains(Keywords.Task.ASSERTION)
    } map {
      case (taskName, taskDef: ConfigObject) => taskName -> taskDef.toConfig.getValue(Keywords.Task.ASSERTION)
    }
  }

}




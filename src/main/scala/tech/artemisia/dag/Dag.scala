package tech.artemisia.dag

import com.typesafe.config._
import tech.artemisia.core.AppLogger._
import tech.artemisia.core.BasicCheckpointManager.CheckpointData
import tech.artemisia.core.Keywords.Task
import tech.artemisia.core._
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.util.Try

/**
  * Created by chlr on 1/3/16.
  */

private[dag] class Dag(node_list: Seq[Node], checkpointData: CheckpointData) {

  this.resolveDependencies(node_list)
  debug("resolved all task dependency")
  var graph = topSort(node_list)
  debug("no cyclic dependency detected")

  def updateNodePayloads(code: Config) = {
    Dag.extractTaskNodes(code) foreach {
      case (node_name, payload) => this.getNodeByName(node_name).payload = payload
    }
  }

  /**
    * Edits the current DAG when runnable nodes have iterations to expand
    * or worklets to import.
    * @param code application config payload
    * @return
    */
  def editDag(code: Config): Config = {
    this.getRunnableNodes.filter(DagEditor.requireEditing) match {
      case Nil => code
      case nodes =>
        editDag(nodes.foldLeft(code) {
          (carry, node) =>
            val (newNodes, newConfig) = DagEditor.editDag(node, code)
            DagEditor.replaceNode(this, node, newNodes, checkpointData) // performing side-effects inside a map function.
            newConfig withFallback carry.withoutPath(s""""${node.name}"""")
        })
    }
  }

  def setNodeStatus(nodeName: String, status: Status.Value) = {
    this.getNodeByName(nodeName).setStatus(status)
  }

  def getNodeByName(name: String) = {
    val node = graph filter { _.name == name }
    assert(node.size == 1, s" A single node by the name $name must exist")
    node.head
  }

  /**
    * get sequence of tasks whose dependencies have met.
    *
    * @param appContext
    * @return
    */
  def getRunnableTasks(appContext: AppContext) = {
    appContext.payload = editDag(appContext.payload)
    for (node <- this.getRunnableNodes) yield {
      node.name -> Try(node.getNodeTask(graph.foldLeft(appContext.payload) {
        (carry: Config, inputNode: Node) => carry.withoutPath(s""""${inputNode.name}"""")
      }, appContext))
    }
  }

  /**
    * get the sequence of nodes whose all parents nodes have already succeeded.
    *
    * @return
    */
  def getRunnableNodes: Seq[Node] = {
    graph map {
      x => checkpointData.taskStatRepo.get(x.name) match {
        case Some(stat) if x.getStatus != stat.status  => x.setStatus(stat.status); x
        case _ => x
      }
    } filter {
      _.isRunnable
    }
  }

  def getChildNodes(node: Node) = {
    this.graph filter { x => x.parents contains node }
  }

  def hasCompleted = {
    graph forall { _.isComplete }
  }

  def getNodesWithStatus(status: Status.Value) = {
    graph filter { _.getStatus == status }
  }

  override def toString = graph.toString()

  /**
    * parse node dependencies and link them.
    *
    * @param nodeList
    */
  protected[dag] def resolveDependencies(nodeList: Seq[Node]): Unit = {
    val nodeMap = nodeList.map(x => x.name -> x).toMap
    nodeList map { x => x -> x.payload.getAs[List[String]](Keywords.Task.DEPENDENCY) } filter {
      x => x._2.nonEmpty
    } foreach {
      case (node, dependency) => {
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

  @tailrec
  private def topSort(unsorted_graph: Seq[Node], sorted_graph: Seq[Node] = Nil): Seq[Node] = {
    (unsorted_graph, sorted_graph) match {
      case (Nil, a) => a
      case _ => {
        val open_nodes = unsorted_graph collect {
          case node@Node(_, Nil) => node
          case node@Node(_, parents) if parents forall {
            sorted_graph contains _
          } => node
        }
        if (open_nodes.isEmpty) {
          AppLogger error {
            s"cyclic dependency detected in graph structure $unsorted_graph"
          }
          throw new DagException("Cycles Detected in Dag")
        }
        topSort(unsorted_graph filterNot {
          open_nodes contains _
        }, sorted_graph ++ open_nodes)
      }
    }
  }
}


object Dag {

  def apply(appContext: AppContext) = {
    val node_list = extractTaskNodes(appContext.checkpoints.adhocPayload withFallback appContext.payload) map {
      case (name, payload) => Node(name, payload)
    }
    new Dag(node_list.toList, appContext.checkpoints)
  }


  /**
    * This method parses a given Config Object and identifies task definition nodes and filters rest.
    *
    * for eg: consider below node the result will be Map("task" -> SimpleConfigObject())
    * here the node foo = bar would be filtered and task and its corresponding configobject value is selected.
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
  private[dag] def extractTaskNodes(config: Config): Map[String, Config] = {
   val data = config.root().asScala filterNot {
      case (key, value) => key.startsWith("__") && key.endsWith("__")
    } filter {
      case (key, value) =>
        value.valueType() == ConfigValueType.OBJECT
      case _ => false
    } filter {
      case (key, value: ConfigObject) =>
        value.toConfig.hasPath(Task.COMPONENT) && value.toConfig.hasPath(Keywords.Task.TASK)
    } map {
     case (key, value: ConfigObject) => key -> value.toConfig
      }
    data.toMap
  }

  def apply(node_list: Seq[Node], checkpointData: CheckpointData = CheckpointData()) = {
    new Dag(node_list, checkpointData)
  }

}




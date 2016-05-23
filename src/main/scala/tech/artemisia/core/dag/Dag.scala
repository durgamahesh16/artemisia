package tech.artemisia.core.dag

import com.typesafe.config.{Config, ConfigObject, ConfigValueType}
import tech.artemisia.config.AppContext
import tech.artemisia.core.Keywords.Task
import tech.artemisia.core.dag.Message.TaskStats
import tech.artemisia.core.{AppLogger, Keywords}
import tech.artemisia.task.TaskContext
import tech.artemisia.util.HoconConfigUtil.{Handler, configToConfigEnhancer}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.{LinearSeq, mutable}

/**
 * Created by chlr on 1/3/16.
 */

private[dag] class Dag(node_list: LinearSeq[Node], checkpoints: mutable.Map[String,TaskStats]) extends Iterable[LinearSeq[Node]] {

  this.resolveDependencies(node_list)
  AppLogger debug "resolved all task dependency"
  val graph = topSort(node_list)
  AppLogger debug "no cyclic dependency detected"
  this.applyCheckpoints(checkpoints)

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

  private def applyCheckpoints(checkpoints: mutable.Map[String,TaskStats]): Unit = {
    AppLogger info "applying checkpoints"
    checkpoints foreach {
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

  def apply(app_context: AppContext) = {
   val node_list = parseNodeFromConfig(app_context.payload) map { case (name,payload) => Node(name,payload) }
   new Dag(node_list.toList,app_context.checkpoints)
  }

  def apply(node_list: LinearSeq[Node], checkpoints: mutable.Map[String,TaskStats] = mutable.Map()) = {
    new Dag(node_list,checkpoints)
  }

  def parseNodeFromConfig(code: Config): Map[String, Config] = {
    TaskContext.payload = code.hardResolve
    (
      TaskContext.payload.root() filterNot {
        case (key, value) => key.startsWith("__") && key.endsWith("__")
      } filter {
        case (key, value)  =>
          value.valueType() == ConfigValueType.OBJECT
        case _ => false
      } filter {
        case (key, value: ConfigObject) =>
          value.toConfig.hasPath(Task.COMPONENT) && value.toConfig.hasPath(Keywords.Task.TASK)
      } map {  case (name, body: ConfigObject) => name -> body.toConfig }
      ).toMap
  }

}




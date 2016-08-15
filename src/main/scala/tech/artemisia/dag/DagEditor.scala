package tech.artemisia.dag

import java.io.File
import scala.collection.JavaConverters._
import com.typesafe.config._
import tech.artemisia.core.Keywords
import tech.artemisia.util.HoconConfigUtil.Handler
/**
  * Created by chlr on 8/12/16.
  */

object DagEditor {

  def editDag(node: Node): Seq[Node] = {
    if(node.payload.hasPath(Keywords.Task.ITERATE)) {
      expandIterableNode(node)
    }
    else if (
      (node.payload.getString(Keywords.Task.COMPONENT) == Keywords.DagEditor.Component)
      && (node.payload.getString(Keywords.Task.COMPONENT) == Keywords.DagEditor.Component)
    ) {
      Seq[Node]()
    }
    Seq[Node]()
  }




  /**
    * inspects and confirms if a node requires editing.
    * editing could be such as
    *  * expanding iterable nodes
    *  * importing worklets
    * @param node node to be inspected
    * @return boolean value to indicate result
    */
  def requireEditing(node: Node) = {
    node.payload.hasPath(Keywords.Task.ITERATE)
  }


  /**
    * expand iterable node to sequence of node
    * @param node node to be expanded
    * @return sequence of the expanded nodes
    */
  def expandIterableNode(node: Node) = {
    val (configList: ConfigList, groupSize: Int) = node.payload.getValue(Keywords.Task.ITERATE) match {
      case x: ConfigList => x -> 1
      case x: ConfigObject =>  x.toConfig.getList("values") -> x.toConfig.as[Int]("group")
      case _ => throw new RuntimeException(s"invalid config for ${Keywords.Task.ITERATE} for node ${node.name}")
    }
    val nodes = for (i <- 1 to configList.size) yield {
      val config = ConfigFactory.empty().withValue(Keywords.Task.VARIABLES, configList.get(i - 1))
        .withFallback(node.payload.withoutPath(Keywords.Task.ITERATE))
      val name = s"${node.name}$$$i"
      Node(name, config)
    }
    nodes.sliding(groupSize,groupSize).sliding(2,1) foreach {
      case parents :: children :: Nil => for(child <- children) { child.parents = parents }
    }
    nodes
  }


  def importModule(node: Node) = {
    val file = new File(node.payload.as[String](s"${Keywords.Task.PARAMS}.module"))
    val module = ConfigFactory parseFile file
    val nodeMap = Dag.parseNodesFromConfig(module)
    val nodes = nodeMap map {
      case (name, payload) =>
        Node(
          s"${node.name}$$$name",
          payload.withValue(Keywords.Task.DEPENDENCY
          ,ConfigValueFactory.fromIterable(payload.getAs[List[String]](Keywords.Task.DEPENDENCY)
                .map(x => s""""${node.name}$$$x"""").toIterable.asJava)
          )
        )
    }
    // we create a mini dag to
    // * identify cycles in the Dag
    // * link all nodes
    Dag(nodes.toSeq)
    nodes
  }


  def replaceNode(dag: Dag ,node: Node, nodes: Seq[Node], currentConfig: Config) = {
    nodes filter ( _.parents == Nil ) foreach {
      x => x.parents = node.parents
    }
    dag.graph filter { _.parents contains node } foreach {
      case x =>  x.parents = x.parents.filterNot(_ == node) ++  nodes filter { x => !nodes.exists(_.parents contains x) }
    }

    dag.graph = dag.graph.filterNot(_ == node) ++ nodes
    dag.graph.foldLeft(currentConfig.withoutPath(node.name)) {
      case (carry: Config, input: Node) => ConfigFactory.empty().withValue(s""""${input.name}"""",input.payload.root())
            .withFallback(carry)
    }
  }

}

package tech.artemisia.dag

import java.io.File

import com.typesafe.config._
import tech.artemisia.core.Keywords
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.collection.JavaConverters._
/**
  * Created by chlr on 8/12/16.
  */

object DagEditor {

  def editDag(node: Node, jobConfig: Config): (Seq[Node],Config) = {
    if(node.payload.hasPath(Keywords.Task.ITERATE))
      expandIterableNode(node)
    else if ((node.payload.getString(Keywords.Task.COMPONENT) == Keywords.DagEditor.Component) &&
      (node.payload.getString(Keywords.Task.COMPONENT) == Keywords.DagEditor.Component)
    ) {
      importModule(node, jobConfig)
    }
    else
      throw new DagException(s"failed to expand/edit node ${node.name}")
  }


  /**
    * inspects and confirms if a node requires editing.
    * editing could be such as
    *  * expanding iterable nodes
    *  * importing worklets
    *
    * @param node node to be inspected
    * @return boolean value to indicate result
    */
  def requireEditing(node: Node) = {
    node.payload.hasPath(Keywords.Task.ITERATE) ||
      ((node.payload.getString(Keywords.Task.COMPONENT) == Keywords.DagEditor.Component) &&
        (node.payload.getString(Keywords.Task.COMPONENT) == Keywords.DagEditor.Component))
  }


  /**
    * expand iterable node to sequence of node
    *
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
    val outputConfig = nodes.foldLeft(ConfigFactory.empty) {
      case (carry, inputNode) => carry.withFallback(ConfigFactory.empty.withValue(s""""${inputNode.name}"""",
        inputNode.payload.root()))
    }
    nodes -> outputConfig
  }


  def importModule(node: Node, jobConfig: Config) = {

    var module = node.payload.as[Config](Keywords.Task.PARAMS).root()
      .keySet().asScala.toList match {
      case "file" :: Nil => ConfigFactory parseFile new File(node.payload.as[String](s"${Keywords.Task.PARAMS}.file"))
      case "node" :: Nil =>
        jobConfig.getConfig(Keywords.Config.WORKLET).as[Config](node.payload.as[String](s"${Keywords.Task.PARAMS}.node"))
      case _ => throw new IllegalArgumentException("file and node are the only supported nodes for Dag Import task")
    }
    val nodeMap = Dag.extractTaskNodes(module)
    val nodes = nodeMap.toSeq map {
      case (name, payload) =>
        // performing sideeffect in map operation.
        module = module.withoutPath(name).withValue(s""""${node.name}$$$name"""", processImportedNode(payload, node).root())
        Node(s"${node.name}$$$name", processImportedNode(payload, node))
    }
    Dag(nodes) // we create a dag object to link nodes and identify cycles.
    node.payload.getAs[ConfigValue](Keywords.Task.ASSERTION) foreach {
      assertion => nodes.filterNot(x => nodes.exists(_.parents contains x))
        .foreach(x => x.payload.withValue(Keywords.Task.ASSERTION, assertion))
    }
    nodes -> module
  }


  private def processImportedNode(importedNodePayLoad: Config, parentNode: Node) = {
    var result = importedNodePayLoad
    importedNodePayLoad.getAs[List[String]](Keywords.Task.DEPENDENCY) foreach {
      dependency: List[String] =>
        result = importedNodePayLoad.withValue(Keywords.Task.DEPENDENCY
        ,ConfigValueFactory.fromIterable(dependency.map(x => s"${parentNode.name}$$$x").asJava))
    }
    // Assertion is not added here because the assertion node must be added only in the last node(s) of the  worklet
    val taskSettingNodes = Seq(Keywords.Task.IGNORE_ERROR, Keywords.Task.COOLDOWN, Keywords.Task.ATTEMPT, Keywords.Task.CONDITION
      ,Keywords.Task.VARIABLES, Keywords.Task.ASSERTION)
    taskSettingNodes.foldLeft(result) {
      (config: Config, inputNode: String) => parentNode.payload.getAs[ConfigValue](inputNode)
        .map{x => config.withValue(inputNode, x)}
        .getOrElse(config)
    }
  }


  /**
    * update the graph of the Dag by replacing editable Dag node with
    * expanded nodes.
    * @param dag Dag to be updated.
    * @param node node to replaced
    * @param nodes new nodes replacing the node
    */
  def replaceNode(dag: Dag ,node: Node, nodes: Seq[Node]) = {
    nodes filter ( _.parents == Nil ) foreach {
      x => x.parents = node.parents
    }
    dag.graph filter { _.parents contains node } foreach {
      case x =>  x.parents = x.parents.filterNot(_ == node) ++  nodes filter { x => !nodes.exists(_.parents contains x) }
    }
    dag.graph = dag.graph.filterNot(_ == node) ++ nodes
  }

}

package tech.artemisia.dag

import com.typesafe.config._
import tech.artemisia.core.Keywords
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/12/16.
  */

object DagEditor {

  def editDag(dag: Dag, payload: Config) = {
    var resultPayload = payload
    for( node <- dag.getRunnableNodes) {
      if(node.payload.hasPath(Keywords.Task.ITERATE)) {
        val newNodes = expandIterableNode(node, node.parents, dag.getChildNodes(node))
        dag.graph = dag.graph.filterNot(_ == node) ++ newNodes
        resultPayload = newNodes.foldLeft(payload.withoutPath(node.name)) { (carry, input) => input.payload.withFallback(carry) }
      }
    }
    resultPayload
  }


  /**
    *
    * @param node
    * @param parents
    * @param children
    * @return
    */
  def expandIterableNode(node: Node, parents: Seq[Node], children: Seq[Node]) = {
    val (configList: ConfigList, groupSize: Int) = node.payload.getValue(Keywords.Task.ITERATE) match {
      case x: ConfigList => x -> 1
      case x: ConfigObject =>  x.toConfig.getList("values") -> x.toConfig.as[Int]("group")
      case _ => throw new RuntimeException(s"invalid config for ${Keywords.Task.ITERATE} for node ${node.name}")
    }

    val nodes: Seq[Node] = for (i <- 1 to configList.size) yield {
      val config = node.payload
        .withoutPath(Keywords.Task.ITERATE)
        .withFallback(ConfigFactory.empty().withValue(Keywords.Task.VARIABLES, configList.get(i - 1)))
      val name = s"${node.name}$$$i"
      Node(name, config)
    }

    (nodes.grouped(groupSize).toSeq :+ children) zip
      (parents +: nodes.grouped(groupSize).toSeq) foreach {
      case (nodeList, parentList) =>
        for (targetNode <- nodeList; parent <- parentList) {
          mergeNodeDependencies(targetNode, parent, node)
        }
    }
    nodes
  }

  /**
    * replace a new parent with a old parent if already exists or add new parent
    *
    * @param targetNode node to be updated
    * @param newParent new parent to be set
    * @param oldParent old parent to be replace if exists.
    */
  def mergeNodeDependencies(targetNode: Node, newParent: Node, oldParent: Node) = {
    targetNode.parents = targetNode.parents.find(x => x == oldParent) match {
      case Some(x) => (targetNode.parents filterNot (x => x == oldParent)) :+ newParent
      case None => targetNode.parents :+ newParent
    }
  }


}

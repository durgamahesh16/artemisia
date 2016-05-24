package tech.artemisia.dag

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec
import tech.artemisia.core.Keywords

/**
 * Created by chlr on 1/23/16.
 */
class NodeSpec extends TestSpec {

    "Node" must "correctly identify if it can run" in {
      val nodes =  NodeSpec.makeNode("node1") :: NodeSpec.makeNode("node2", "[node1]") :: NodeSpec.makeNode("node3","[node2]") ::
        NodeSpec.makeNode("node4","[node3]") :: NodeSpec.makeNode("node5","[node4]") :: Nil
      nodes.sliding(2) foreach {
        case node1 :: node2 :: Nil => node2.parents = node1 :: Nil
        case _ =>
      }
      nodes.head.setStatus(Status.SUCCEEDED)
      nodes(1).isRunnable must be (true)
      nodes(2).isRunnable must be (false)
      nodes.head.isRunnable must be (false)
    }

  "Node" must "respect the laws of node equality" in {
    val node1 = NodeSpec.makeNode("testnode")
    val node2 = NodeSpec.makeNode("testnode")
    node1 must equal (node2)
  }

  "Node" must "apply checkpoint accordingly" in {
    val node1 = NodeSpec.makeNode("testnode")
    node1.applyStatusFromCheckpoint(Status.FAILED)
    node1.getStatus must be (Status.READY)
    node1.applyStatusFromCheckpoint(Status.SUCCEEDED)
    node1.getStatus must be (Status.SUCCEEDED)
    node1.applyStatusFromCheckpoint(Status.SKIPPED)
    node1.getStatus must be (Status.SKIPPED)
  }

}

object NodeSpec {

  def makeNode(name: String, dependencies: String = "[]") =
    Node(name,ConfigFactory.parseString(s"${Keywords.Task.DEPENDENCY} = $dependencies"))
}
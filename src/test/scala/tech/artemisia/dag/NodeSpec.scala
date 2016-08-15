package tech.artemisia.dag

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec
import tech.artemisia.core.Keywords
import scala.collection.JavaConverters._

/**
  * Created by chlr on 1/23/16.
  */
class NodeSpec extends TestSpec {

  "Node" must "correctly identify if it can run" in {
    val nodes = NodeSpec.makeNode("node1") :: NodeSpec.makeNode("node2", "[node1]") :: NodeSpec.makeNode("node3", "[node2]") ::
      NodeSpec.makeNode("node4", "[node3]") :: NodeSpec.makeNode("node5", "[node4]") :: Nil
    nodes.sliding(2) foreach {
      case node1 :: node2 :: Nil => node2.parents = node1 :: Nil
      case _ =>
    }
    nodes.head.setStatus(Status.SUCCEEDED)
    nodes(1).isRunnable must be(true)
    nodes(2).isRunnable must be(false)
    nodes.head.isRunnable must be(false)
  }

  it must "respect the laws of node equality" in {
    val node1 = NodeSpec.makeNode("testnode")
    val node2 = NodeSpec.makeNode("testnode")
    node1 must equal(node2)
  }

  it must "apply checkpoint accordingly" in {
    val node1 = NodeSpec.makeNode("testnode")
    node1.applyStatusFromCheckpoint(Status.FAILED)
    node1.getStatus must be(Status.READY)
    node1.applyStatusFromCheckpoint(Status.SUCCEEDED)
    node1.getStatus must be(Status.SUCCEEDED)
    node1.applyStatusFromCheckpoint(Status.SKIPPED)
    node1.getStatus must be(Status.SKIPPED)
  }

  it must "update payload when dependencies are changed" in {
    val payload1 = ConfigFactory parseString """{"step1":{"Component":"TestComponent","Task":"TestAdderTask","params":{"num1":1,"num2":2,"result_var":"tango1"}}}"""
    val payload2 = ConfigFactory parseString """{"step1":{"Component":"TestComponent","Task":"TestAdderTask","params":{"num1":1,"num2":2,"result_var":"tango1"}}}"""
    val node1 = Node("node1", payload1)
    val node2 = Node("node2", payload2)
    node2.parents = Seq(node1)
    node2.payload.getList(Keywords.Task.DEPENDENCY).unwrapped().asScala must be (Seq("node1"))
    
  }

}

object NodeSpec {

  def makeNode(name: String, dependencies: String = "[]") =
    Node(name, ConfigFactory.parseString(s"${Keywords.Task.DEPENDENCY} = $dependencies"))
}
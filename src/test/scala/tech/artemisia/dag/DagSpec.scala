package tech.artemisia.dag

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import Message.TaskStats
import tech.artemisia.TestSpec
import tech.artemisia.util.Util

import scala.collection.mutable

/**
 * Created by chlr on 1/3/16.
 */
class DagSpec extends TestSpec {


  "The Dag" must "handle sequentially dependent nodes appropriately" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2", "[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4","[node3]")
    val node5 = NodeSpec.makeNode("node5","[node4]")
    val node6 = NodeSpec.makeNode("node6","[node5]")
    val node7 = NodeSpec.makeNode("node7","[node6]")
    val graph = node1 :: node2 :: node3 :: node4 :: node5 :: node6 :: node7 :: Nil
    val dag = Dag(graph)

    info("inspecting Dag size")
    dag.size must be (7)
    info("Dag must be in same order as in source")
    for (i <- 1 to 7; current_node: Node <- dag.getRunnableNodes) {
      current_node must be (graph(i-1))
      current_node.setStatus(Status.SUCCEEDED)
    }
  }


  it must "identify cycles" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1,node4]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4", "[node3]")
    val graph = node1 :: node2 :: node3 :: node4 :: Nil
    val ex = intercept[DagException] {
      Dag(graph)
    }
    ex.getMessage must be ("Cycles Detected in Dag")
  }

  it must "report invalid dependency references" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1,node5]")
    val node3 = NodeSpec.makeNode("node3", "[node2]")
    val node4 = NodeSpec.makeNode("node4", "[node3]")
    val graph = node1 :: node2 :: node3 :: node4 :: Nil
    val ex = intercept[DagException] {
      Dag(graph)
    }
    ex.getMessage must be (s"invalid dependency reference for node5 in node2")

  }

  it must "produce right batches of steps to execute" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4","[node2]")
    val node5 = NodeSpec.makeNode("node5","[node4]")
    val node6 = NodeSpec.makeNode("node6","[node4]")
    val node7 = NodeSpec.makeNode("node7","[node5]")
    val graph = node1 :: node2 :: node3 :: node4 :: node5 :: node6 :: node7 :: Nil
    val dag = Dag(graph)

    dag.getRunnableNodes.toSet must be ((node1 :: node2 :: Nil).toSet)
    node1.setStatus(Status.SUCCEEDED)
    dag.getRunnableNodes must be ( node2 :: Nil)
    node2.setStatus(Status.SUCCEEDED)
    dag.getRunnableNodes must be ( node3 :: node4 :: Nil)
    node4.setStatus(Status.SUCCEEDED)
    dag.getRunnableNodes must be ( node3  :: node5 :: node6 :: Nil)

  }

  it must "resolve dependencies approriately" in  {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val dag =  Dag(node1 :: node2 :: node3 :: Nil)
    dag.getNodeByName("node1").parents must be (Nil)
    dag.getNodeByName("node2").parents must be (node1 :: Nil)
    dag.getNodeByName("node3").parents must be (node2 :: Nil)

  }

  it must "apply checkpoints correctly" in {
    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4","[node3]")
    val checkpoints = mutable.Map(
                          "node1" -> DagSpec.taskStats("node1",Status.SUCCEEDED),
                          "node2" -> DagSpec.taskStats("node2",Status.SUCCEEDED)
                                  )
    val dag = Dag(node1 :: node2 :: node3 :: node4 :: Nil, checkpoints)
    dag.getNodeByName("node1").getStatus must be (Status.SUCCEEDED)
    dag.getNodeByName("node2").getStatus must be (Status.SUCCEEDED)
    dag.getNodeByName("node3").getStatus must be (Status.READY)
  }

  it must "parse Task Nodes correctly from the ConfigObject" in {

    val code = scala.io.Source.fromFile(this.getClass.getResource("/code/code_with_incorrect_blocks.conf").getFile).mkString
    val nodes = Dag.parseNodeFromConfig(ConfigFactory parseString code)
    val dummy_step1 = ConfigFactory parseString "{ Component: Dummy, Task: DummyTask ,params: { dummy_param1 = 11, dummy_param2 = no } }"
    nodes must be (mutable.Map("dummy_step1" -> dummy_step1))

  }

  it must "update each node with new payload" in {
    val dag = DagSpec.makeDagFromFile(this.getClass.getResource("/code/code_with_incorrect_blocks.conf").getFile)
    val content = ConfigFactory parseString
      """
        |dummy_step1 = { Component: Dummy, Task: DummyTask, params: {new_key = new_value } }
      """.stripMargin
    dag.updateNodePayloads(content)
    dag.getNodeByName("dummy_step1").payload.getString("params.new_key") must equal ("new_value")
  }

  it must "know if all nodes have completed execution and lookup nodes with given status" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val dag =  Dag(node1 :: node2 :: node3 :: Nil)
    dag.getNodeByName("node1").setStatus(Status.SUCCEEDED)
    dag.hasCompleted must be (false)
    dag.getNodeByName("node2").setStatus(Status.SKIPPED)
    dag.hasCompleted must be (false)
    dag.getNodeByName("node3").setStatus(Status.SUCCEEDED)
    dag.hasCompleted must be (true)
    dag.getNodesWithStatus(Status.SKIPPED) must be (node2 :: Nil)
    dag.getNodesWithStatus(Status.SUCCEEDED) must be (node1 :: node3 :: Nil)

  }

}

object DagSpec {

  def taskStats(name: String, status: Status.Value) = {
    TaskStats(startTime = Util.currentTime, endTime = Util.currentTime, status =  status, attempts = 1, taskOutput = ConfigFactory.empty())
  }

  def makeDagFromFile(file: String) = {
    val config = ConfigFactory.parseFile(new File(file))
    val node_list: Iterable[Node] = Dag.parseNodeFromConfig(config) map {
      case (name: String, body: Config) => Node(name,body)
    }
    Dag(node_list.toList)
  }

}

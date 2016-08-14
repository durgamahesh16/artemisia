package tech.artemisia.dag

import java.io.File

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
  * Created by chlr on 8/13/16.
  */
class DagEditorSpec extends TestSpec {


  "DagEditor" must "expand iterable nodes" in {
    val file = this.getClass.getResource("/code/iteration_test.conf").getFile
    val config = ConfigFactory parseFile new File(file)
    val node1 = Node("step1", config.getConfig("step1"))
    val node2 = Node("step2", config.getConfig("step2"))
    val node3 = Node("step3", config.getConfig("step3"))
    val node4 = Node("step4", config.getConfig("step4"))
    node3.parents = Seq(node1, node2)
    node4.parents = Seq(node3)
    val Seq(node3a, node3b, node3c) = DagEditor.expandIterableNode(node3, Seq(node1, node2), Seq(node4))
    node1.parents must be (Seq())
    node2.parents must be (Seq())
    node3a.parents must be (Seq(node1, node2))
    node3b.parents must be (Seq(node1, node2))
    node3c.parents must be (Seq(node3a, node3b))
    node4.parents must be (Seq(node3c))
  }

}

package tech.artemisia.dag

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.TestSpec
import tech.artemisia.core.{BasicCheckpointManager, Keywords}
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.collection.JavaConverters._

/**
  * Created by chlr on 8/13/16.
  */
class DagEditorSpec extends TestSpec {


  "DagEditor" must "expand iterable nodes" in {
    val file = this.getClass.getResource("/code/iteration_test.conf").getFile
    val config = ConfigFactory parseFile new File(file)

    val node4 = Node("step4", config.getConfig("step4"))
    val node1 = Node("step1", config.getConfig("step1"))
    val node2 = Node("step2", config.getConfig("step2"))
    val node3 = Node("step3", config.getConfig("step3"))

    val dag = new Dag(Seq(node1, node2, node3, node4), BasicCheckpointManager.CheckpointData(ConfigFactory.empty(), Map()))

    val Seq(node3a, node3b, node3c) = DagEditor.expandIterableNode(node3)
    node3a.parents must be (Seq())
    node3b.parents must be (Seq())
    node3c.parents must be (Seq(node3a, node3b))

    val modifiedConfig = DagEditor.replaceNode(dag, node3, Seq(node3a, node3b, node3c), config)
    modifiedConfig.getAs[Config]("step3") must be (None)
    modifiedConfig.getList(s""""step3$$1".${Keywords.Task.DEPENDENCY}""").unwrapped.asScala must be (Seq("step1", "step2"))
    modifiedConfig.getList(s""""step3$$2".${Keywords.Task.DEPENDENCY}""").unwrapped.asScala must be (Seq("step1", "step2"))
    modifiedConfig.getList(s""""step3$$3".${Keywords.Task.DEPENDENCY}""").unwrapped.asScala must be (Seq("step3$1", "step3$2"))
    modifiedConfig.getList(s"step4.${Keywords.Task.DEPENDENCY}").unwrapped.asScala must be (Seq("step3$3"))
  }

}

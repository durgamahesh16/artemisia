package tech.artemisia.task

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created by chlr on 1/26/16.
 */

class TestComponent(name: String) extends Component(name) {

  override val info: String = "This is a TestComponent"
  override val doc: String = "this is TestComponent doc"
  override val tasks: Seq[TaskLike] = Seq(TestAdderTask, TestFailTask)
  override val defaultConfig: Config = ConfigFactory parseString
    s"""
       | {
       |   ckey = cval
       | }
     """.stripMargin

}

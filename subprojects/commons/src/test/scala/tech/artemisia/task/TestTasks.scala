package tech.artemisia.task

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 1/26/16.
 */
class TestAdderTask(name: String ,val num1: Int, val num2: Int, val result: String) extends Task(name) {

  override def setup(): Unit = {}
  override def work(): Config = { ConfigFactory parseString s"$result = ${num1 + num2}" }
  override def teardown(): Unit = {}

}

object TestAdderTask extends TaskLike {

  override def apply(name: String, param: Config) = {
    new TestAdderTask(name, param.as[Int]("num1"),param.as[Int]("num2"),param.as[String]("result_var"))
  }

  override val taskName: String = "TestAdderTask"
  override def doc(component: String): String = "TestAdderTask is a test addition task"
  override val info: String = "test add task"
  override val desc: String = ""
  override val paramConfigDoc = ConfigFactory.empty()
  override val fieldDefinition = Map[String, AnyRef]()
  override val defaultConfig: Config = ConfigFactory parseString
    s"""
       | {
       |   tkey1 = tval1
       | }
     """.stripMargin
}


class TestFailTask(name: String) extends Task(name) {

  override def setup(): Unit = {}
  override def work(): Config = { throw new Exception("FailTask always fail") }
  override def teardown(): Unit = {}

}

object TestFailTask extends TaskLike {

  override def apply(name: String, param: Config) = {
    new TestFailTask(name)
  }
  override val taskName: String = "TestFailTask"
  override def doc(component: String): String = "This is a sample test task that always fail"
  override val info: String = "test fail task"
  override val desc: String = ""
  override val paramConfigDoc = ConfigFactory.empty()
  override val fieldDefinition = Map[String, AnyRef]()
  override val defaultConfig: Config = ConfigFactory parseString
    s"""
       | {
       |   tkey2 = tval2
       | }
     """.stripMargin
}

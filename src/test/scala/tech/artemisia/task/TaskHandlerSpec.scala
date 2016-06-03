package tech.artemisia.task


import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import tech.artemisia.TestSpec

import scala.util.{Failure, Success}

/**
  * Created by chlr on 6/2/16.
  */
class TaskHandlerSpec extends TestSpec {

  "TaskHandler" must "handle assertions failure appropriately" in {
      val task = new Task("dummy_task") {
        override protected[task] def setup(): Unit = ()
        override protected[task] def work(): Config = ConfigFactory parseString "foo = 100"
        override protected[task] def teardown(): Unit = ()
      }
      val taskConfig = TaskConfig(assertion = Some((ConfigValueFactory.fromAnyRef("${foo} == 0"),"test")))
      val handler = new TaskHandler(taskConfig, task)
      val ex = intercept[AssertionError] {
        handler.execute()
      }
      ex.getMessage must be ("assertion failed: test")
  }


  it must "task output must correctly appear in assertions" in {

    val task = new Task("dummy_task") {
      override protected[task] def setup(): Unit = ()
      override protected[task] def work(): Config = ConfigFactory parseString "foo = 100"
      override protected[task] def teardown(): Unit = ()
    }
    val taskConfig = TaskConfig(assertion = Some((ConfigValueFactory.fromAnyRef("${foo} == 100"),"test")))
    val handler = new TaskHandler(taskConfig, task)
    handler.execute() match {
      case Success(config) => config.getInt("foo") must be (100)
      case Failure(th) => throw th
    }
  }

}

package tech.artemisia.task

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.TestSpec
import tech.artemisia.dag.Status

import scala.concurrent.duration._
/**
 * Created by chlr on 1/23/16.
 */
class GenericTaskSpec extends TestSpec {


  "The Task" must "retry a failing task for configured number of times" in {
    val task_config = GenericTaskSpec.getDefaultTaskConfig

    val fail_task = new Task("testTask") {
      override def work(): Config = { ConfigFactory.empty() }
      override def setup(): Unit = { throw new Exception("fail") }
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(task_config,fail_task)
    val result = task.execute()
    result.isFailure must be (true)
    result.failed.get mustBe an [Exception]
    task.getAttempts must be (3)
    task.getStatus must be (Status.FAILED)
  }

  it must "stop retrying if it succeeds before retry limit" in {

    val task_config = GenericTaskSpec.getDefaultTaskConfig
     val succeed_on_2_attempt_task = new Task("testTask") {
       var attempts = 1
       override def setup(): Unit = {}
       override def work(): Config = { if (this.attempts == 1) {  attempts += 1 ; throw new Exception("") } else { ConfigFactory.empty() } }
       override def teardown(): Unit = {}
     }
    val task = new TaskHandler(task_config,succeed_on_2_attempt_task)
    task.execute()
    task.getAttempts must be (2)
  }

  it must "ignore failures in teardown phase" in {
    val task_config = GenericTaskSpec.getDefaultTaskConfig
    val fail_on_teardown_task = new Task("testTask") {
      override def setup(): Unit = {}
      override def work(): Config = {  ConfigFactory.empty }
      override def teardown(): Unit = { throw new Exception("my own exception") }
    }
    val task = new TaskHandler(task_config,fail_on_teardown_task)
    val result = task.execute()
    result.get must equal (ConfigFactory.empty)
  }

  it must "skip execution if the task_option flag is set" in {
    val task_config = TaskConfig(taskName = "test_task",retryLimit = 3, cooldown = 10 milliseconds, conditions = Some(false -> ""))
    val a_dummy_task = new Task("testTask") {
      override def setup(): Unit = { throw new Exception("my own exception") }
      override def work(): Config = {  throw new Exception("my own exception") }
      override def teardown(): Unit = { throw new Exception("my own exception") }
    }
    val task = new TaskHandler(task_config,a_dummy_task)
    val result = task.execute()
    task.getStatus must be (Status.SKIPPED)
    task.getAttempts must be (0)
    result.get must equal (ConfigFactory.empty)
  }


}


object GenericTaskSpec {

  def getDefaultTaskConfig = {
     TaskConfig(taskName = "test_task",retryLimit = 3, cooldown = 10 milliseconds)
  }

}

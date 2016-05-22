package tech.artemisia.core.dag

import akka.actor.Props
import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.ActorTestSpec
import tech.artemisia.core.Keywords
import tech.artemisia.core.dag.Message.{TaskFailed, TaskStats, TaskSuceeded, TaskWrapper}
import tech.artemisia.task.{GenericTaskSpec, Task, TaskHandler}
import scala.concurrent.duration._

/**
 * Created by chlr on 1/25/16.
 */
class WorkerActorSpec extends ActorTestSpec {

  "The worker" must "send TaskSuceeded message when the task completes successfully" in {

    val worker = system.actorOf(Props[Worker])
    val test_task = new Task("testTask") {
      override def setup(): Unit = {}
      override def work(): Config = {ConfigFactory.empty()}
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(GenericTaskSpec.getDefaultTaskConfig,test_task)
    within(1 seconds) {
      worker ! TaskWrapper("test_task", task)
      expectMsgPF(1 second) {
        case TaskSuceeded("test_task",TaskStats(_,_,Status.SUCCEEDED,1,_)) => ()
      }
    }

  }

  it must "send TaskFailed message when the task fails" in {

    val worker = system.actorOf(Props[Worker])
    val test_task = new Task("testTask"){

      override def setup(): Unit = { throw new Exception("my known exception") }
      override def work(): Config = {ConfigFactory.empty()}
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(GenericTaskSpec.getDefaultTaskConfig,test_task)

      within(1 seconds) {
      worker ! TaskWrapper("test_task", task)
      expectMsgPF(1 second) {
        case TaskFailed("test_task",TaskStats(_,_,Status.FAILED,3,_),_) => ()
      }
    }
  }


  it must s"set status to FAILURE_IGNORED when ${Keywords.Task.IGNORE_ERROR} is set" in {
    val worker = system.actorOf(Props[Worker])
    val test_task = new Task("testTask"){

      override def setup(): Unit = { throw new Exception("my known exception") }
      override def work(): Config = {ConfigFactory.empty()}
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(GenericTaskSpec.getDefaultTaskConfig.copy(ignoreFailure = true),test_task)

    within(1 seconds) {
      worker ! TaskWrapper("test_task", task)
      expectMsgPF(1 second) {
        case TaskFailed("test_task",TaskStats(_,_,Status.FAILURE_IGNORED,3,_),_) => ()
      }
    }

  }


}

package tech.artemisia.dag

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool
import akka.testkit.TestProbe
import tech.artemisia.ActorTestSpec
import tech.artemisia.core.{AppContext, AppSetting}
import tech.artemisia.dag.Message.{Messageable, TaskStats, _}
import tech.artemisia.task.{TaskHandler, TestAdderTask, TestFailTask}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.concurrent.duration._

/**
 * Created by chlr on 1/25/16.
 */

class DagPlayerSpec extends ActorTestSpec {

  var workers: ActorRef = _
  var probe: DagPlayerSpec.DagProbe = _
  var app_settings: AppSetting = _
  var dag: Dag = _
  var dag_player: ActorRef = _
  var app_context: AppContext = _

  override def beforeEach() = {
    workers = system.actorOf(RoundRobinPool(1).props(Props[Worker]))
    probe = DagPlayerSpec.getTestProbe(system)
  }


  "DagPlayer" must "execute all tasks in the Dag" in {

    setUpArtifacts(this.getClass.getResource("/code/multi_step_addition_job.conf").getFile)
    info("Sending tick 1")
    dag_player ! new Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step1",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSuceeded("step1", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
        stats.taskOutput.as[Int]("tango") must be (30)
      }
    }

    info("Sending tick 1")
    dag_player ! new Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step2",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSuceeded("step2", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
      }
    }
  }


  "DagPlayer" must "must handle error" in {

    setUpArtifacts(this.getClass.getResource("/code/multi_step_addition_with_failure.conf").getFile)
    info("Sending tick 1")
    dag_player ! new Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step1",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSuceeded("step1", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
        stats.taskOutput.as[Int]("tango") must be (20)
      }
    }

    info("Sending tick 1")
    dag_player ! new Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step2",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestFailTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskFailed("step2", stats: TaskStats,exception: Throwable) => {
        stats.status must be(Status.FAILED)
        exception.getMessage must be ("FailTask always fail")
      }
    }

  }


  "DagPlayer" must "handle ignore-error" in {

    setUpArtifacts(this.getClass.getResource("/code/multi_step_with_failure_ignored.conf").getFile)
    info("Sending tick 1")
    dag_player ! new Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step1",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestFailTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskFailed("step1", stats: TaskStats, exception: Throwable) => {
        stats.status must be(Status.FAILURE_IGNORED)
        exception.getMessage must be ("FailTask always fail")
      }
    }

    info("Sending tick 1")
    dag_player ! new Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step2",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSuceeded("step2", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
      }
    }
  }


  it must "handle failures in task initialization" in {
    setUpArtifacts(this.getClass.getResource("/code/incorrect_config.conf").getFile)
    within(1000 millis) {
      dag_player ! new Tick
      expectNoMsg
    }
    dag.getNodeByName("step1").getStatus must be (Status.FAILED)
  }

  it must "apply defaults defined in settings.conf" in {
    setUpArtifacts(this.getClass.getResource("/code/apply_defaults.conf").getFile)
    within(1000 millis) {
      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("step2",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
        case x => info(x.toString)
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("step1", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("foo") must be (50)
        }
      }
    }
  }
  

  def setUpArtifacts(code: String) = {
    app_settings = AppSetting(value = Some(code),skip_checkpoints = true)
    app_context = new AppContext(app_settings)
    dag = Dag(app_context)
    dag_player = system.actorOf(Props(new DagPlayer(dag,app_context,probe.ref)))
  }

}

object DagPlayerSpec {

  class DagProbe(system: ActorSystem) extends TestProbe(system) {

    def validateAndRelay(destination: ActorRef ,duration: Duration = 5 seconds)
                        (validate: PartialFunction[Messageable,Unit])  = {
      this.expectMsgPF(duration) {
        case message: Messageable  => { validate(message)
          destination.tell(message,this.ref)
        }
      }
    }
  }

  def getTestProbe(system: ActorSystem) = new DagProbe(system)
}

package tech.artemisia.dag


import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool
import akka.testkit.TestProbe
import tech.artemisia.ActorTestSpec
import tech.artemisia.core.{AppContext, AppSetting}
import tech.artemisia.dag.Message._
import tech.artemisia.task.{TaskHandler, TestAdderTask}
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by chlr on 8/13/16.
  */
class DagPlayerSpec_3 extends ActorTestSpec {

  var workers: ActorRef = _
  var probe: DagPlayerSpec_3.DagProbe = _
  var app_settings: AppSetting = _
  var dag: Dag = _
  var dag_player: ActorRef = _
  var app_context: AppContext = _

  override def beforeEach() = {
    workers = system.actorOf(RoundRobinPool(1).props(Props[Worker]))
    probe = DagPlayerSpec_3.getTestProbe(system)
  }

  it must "file import worklet" in {
    setUpArtifacts(this.getClass.getResource("/code/worklet_node_import.conf").getFile)
    within(20000 millis) {
      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("task1", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango1") must be (3)
        }
      }

      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task2$step1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("task2$step1", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango") must be (30)
        }
      }

      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task2$step2",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("task2$step2", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("beta") must be (50)
        }
      }

      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task3",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("task3", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango2") must be (3)
        }
      }

      dag_player ! new Tick
      probe.expectNoMsg(2 second)

    }
  }

  def setUpArtifacts(code: String) = {
    app_settings = AppSetting(value = Some(code),skip_checkpoints = true)
    app_context = new AppContext(app_settings)
    dag = Dag(app_context)
    dag_player = system.actorOf(Props(new DagPlayer(dag,app_context,probe.ref)))
  }

}

object DagPlayerSpec_3 {

  class DagProbe(system: ActorSystem) extends TestProbe(system) {

    def validateAndRelay(destination: ActorRef, duration: Duration = 5 seconds)
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


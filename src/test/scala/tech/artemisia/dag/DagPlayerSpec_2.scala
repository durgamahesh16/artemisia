package tech.artemisia.dag

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool
import akka.testkit.TestProbe
import tech.artemisia.ActorTestSpec
import tech.artemisia.core.{AppContext, AppSetting}
import tech.artemisia.dag.Message._
import tech.artemisia.task.{TaskHandler, TestAdderTask}
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.concurrent.duration.{Duration, _}

/**
  * Created by chlr on 8/13/16.
  */
class DagPlayerSpec_2 extends ActorTestSpec {

  var workers: ActorRef = _
  var probe: DagPlayerSpec_2.DagProbe = _
  var app_settings: AppSetting = _
  var dag: Dag = _
  var dag_player: ActorRef = _
  var app_context: AppContext = _

  override def beforeEach() = {
    workers = system.actorOf(RoundRobinPool(1).props(Props[Worker]))
    probe = DagPlayerSpec_2.getTestProbe(system)
  }

//  "DagPlayer" must "apply local variables" in {
//    setUpArtifacts(this.getClass.getResource("/code/local_variables.conf").getFile)
//    within(1000 millis) {
//      dag_player ! new Tick
//      probe.validateAndRelay(workers) {
//        case TaskWrapper("step1",task_handler: TaskHandler) => {
//          task_handler.task mustBe a[TestAdderTask]
//        }
//      }
//      probe.validateAndRelay(dag_player) {
//        case TaskSuceeded("step1", stats: TaskStats) => {
//          stats.status must be (Status.SUCCEEDED)
//          stats.taskOutput.as[Int]("foo") must be (50)
//        }
//      }
//    }
//  }
//
//
//  it must "apply defaults defined in settings.conf" in {
//    setUpArtifacts(this.getClass.getResource("/code/apply_defaults.conf").getFile)
//    within(1000 millis) {
//      dag_player ! new Tick
//      probe.validateAndRelay(workers) {
//        case TaskWrapper("step1",task_handler: TaskHandler) => {
//          task_handler.task mustBe a[TestAdderTask]
//        }
//      }
//      probe.validateAndRelay(dag_player) {
//        case TaskSuceeded("step1", stats: TaskStats) => {
//          stats.status must be (Status.SUCCEEDED)
//          stats.taskOutput.as[Int]("foo") must be (50)
//        }
//      }
//    }
//  }


  it must "handle looping in dag" in {
    setUpArtifacts(this.getClass.getResource("/code/iteration_test.conf").getFile)
    within(1000 millis) {
      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("step1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("step1",stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango1") must be (3)
        }
      }

      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("step2",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("step2",stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango2") must be (3)
        }
      }

      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("step3$1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("step3$1",stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango3a") must be (30)
        }
      }


      dag_player ! new Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("step3$2",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSuceeded("step3$2",stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango3b") must be (300)
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

object DagPlayerSpec_2 {

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


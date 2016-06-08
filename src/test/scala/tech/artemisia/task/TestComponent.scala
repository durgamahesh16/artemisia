package tech.artemisia.task

import com.typesafe.config.Config

/**
 * Created by chlr on 1/26/16.
 */

class TestComponent(name: String) extends Component(name) {

  override def dispatchTask(task: String, name: String, params: Config): Task = {
    task match {
      case "TestAdderTask" =>  TestAdderTask(params)
      case "FailTask" => new TestFailTask()
    }
  }

  override val info: String = "This is a TestComponent"

  override val doc: String = "this is TestComponent doc"

  override def taskDoc(task: String): String = task match {
    case "TestAdderTask" => "TestAdderTask is a test addition task"
  }

}

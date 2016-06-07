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
  override val doc: String = ""
  override def taskDoc(task: String): String = ""
}

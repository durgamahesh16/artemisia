package tech.artemisia.task.localhost

import com.typesafe.config.Config
import tech.artemisia.inventory.exceptions.UnknownTaskException
import tech.artemisia.task.{Component, Task}


/**
 * Created by chlr on 2/21/16.
 */
class CoreComponents extends Component {

  override def dispatchTask(task: String, name: String, config: Config): Task = {
    task match {
      case "ScriptTask" => ScriptTask(name,config)
      case _ => throw new UnknownTaskException(s"task $task is not valid task in Component ${classOf[CoreComponents].getSimpleName}")
    }
  }

  override val doc: String =
    s"""| This components hosts core tasks such as
        | ${classOf[ScriptTask].getSimpleName} => ${ScriptTask.info}""".stripMargin

  /**
    * get documentation of the task
    * @param name name of the task
    */
  override def taskDoc(name: String): String = {
    name match {
      case "ScriptTask" => ScriptTask.doc
      case _ => throw new UnknownTaskException(s"task $task is not valid task in Component ${classOf[CoreComponents].getSimpleName}")
    }
  }
}

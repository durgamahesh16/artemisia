package tech.artemisia.task.database.mysql

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.inventory.exceptions.UnknownTaskException
import tech.artemisia.task.{Component, Task}

/**
 * Created by chlr on 4/8/16.
 */

class MySQLComponent(componentName: String) extends Component(componentName) {

  val defaultConfig = ConfigFactory parseString
    """
      | params: {
      | dsn = { port: 3306 }
      |}
      |
    """.stripMargin



  override def dispatchTask(task: String, name: String, config: Config): Task = {
    task match {
      case "ExportToFile" => ExportToFile(name, config withFallback defaultConfig)
      case "SQLRead" => SQLRead(name, config withFallback defaultConfig)
      case "LoadToTable" => LoadToTable(name, config withFallback defaultConfig)
      case "SQLExecute" => SQLExecute(name, config withFallback defaultConfig)
      case _ => throw new UnknownTaskException(s"task $task is not valid task in Component $componentName")
    }
  }

  override val info = "Component for interacting with MySQL database"

  override val doc: String =
    s"""| Supports interaction with a MySQL Database. Supports Tasks such as
        | ${classOf[ExportToFile].getSimpleName} => ${ExportToFile.info}
        | ${classOf[LoadToTable].getSimpleName} => ${LoadToTable.info}
        | ${classOf[SQLExecute].getSimpleName} => ${SQLExecute.info}
        | ${classOf[SQLRead].getSimpleName} => ${SQLRead.info} """.stripMargin

  /**
    * get documentation of the task
    *
    * @param task name of the task
    */
  override def taskDoc(task: String): String = {
    task match {
      case "ExportToFile" => ExportToFile.doc
      case "SQLRead" => SQLRead.doc
      case "LoadToTable" => LoadToTable.doc
      case "SQLExecute" => SQLExecute.doc
      case _ => throw new UnknownTaskException(s"task $task is not valid task in Component $componentName")
    }
  }
}



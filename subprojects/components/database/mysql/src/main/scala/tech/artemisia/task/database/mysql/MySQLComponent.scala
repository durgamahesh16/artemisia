package tech.artemisia.task.database.mysql

import com.typesafe.config.ConfigFactory
import tech.artemisia.task.{Component, TaskLike}

/**
 * Created by chlr on 4/8/16.
 */

class MySQLComponent(componentName: String) extends Component(componentName) {

  override val defaultConfig = ConfigFactory parseString
    """
      | params: {
      | dsn = { port: 3306 }
      |}
      |
    """.stripMargin


  override val info = "Component for interacting with MySQL database"

  override val doc: String =
    s"""| Supports interaction with a MySQL Database. Supports Tasks such as
        | ${classOf[ExportToFile].getSimpleName} => ${ExportToFile.info}
        | ${classOf[LoadToTable].getSimpleName} => ${LoadToTable.info}
        | ${classOf[SQLExecute].getSimpleName} => ${SQLExecute.info}
        | ${classOf[SQLRead].getSimpleName} => ${SQLRead.info} """.stripMargin

  override val tasks: Seq[TaskLike] = Seq(ExportToFile, LoadToTable, SQLExecute, SQLRead)

}



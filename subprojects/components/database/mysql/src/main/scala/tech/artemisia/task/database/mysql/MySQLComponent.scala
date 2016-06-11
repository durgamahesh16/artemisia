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

  override val tasks: Seq[TaskLike] = Seq(ExportToFile, LoadToTable, SQLExecute, SQLRead)

  override val info: String = "This components provides tasks to interact with a mysql database"

}



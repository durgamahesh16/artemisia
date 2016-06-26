package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.{Component, TaskLike}

/**
  * Created by chlr on 6/26/16.
  */

class TeraComponent(name: String) extends Component(name: String) {

  override val tasks: Seq[TaskLike] = Seq(SQLExecute, SQLRead, LoadToTable, ExportToFile)

  override val defaultConfig: Config = ConfigFactory parseString
    """
      |
    """.stripMargin

  override val info: String = "This Component supports exporting loading and executing queries against Teradata database"

}

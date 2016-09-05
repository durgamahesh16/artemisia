package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.{Component, TaskLike}

/**
  * Created by chlr on 6/26/16.
  */

class TeraComponent(name: String) extends Component(name: String) {

  override val tasks: Seq[TaskLike] = Seq(SQLExecute, SQLRead, LoadFromFile, ExportToFile, ExportToHDFS, LoadFromHDFS,
    TDCHLoad, TDCHExtract)

  override val defaultConfig: Config = ConfigFactory parseString
    """
      | {
      | dsn = { port: 1025 }
      | load.mode = default
      | }
    """.stripMargin

  override val info: String = "This Component supports exporting loading and executing queries against Teradata database"

}

package tech.artemisia.task.database.postgres

import com.typesafe.config.ConfigFactory
import tech.artemisia.task.{Component, TaskLike}

/**
  * Created by chlr on 6/9/16.
  */
class PostgresComponent(name: String) extends Component(name) {

  override val defaultConfig = ConfigFactory parseString
    """
      | {
      | dsn = { port: 5432 }
      | }
    """.stripMargin

  override val tasks: Seq[TaskLike] = Seq(ExportToFile, LoadFromFile, SQLExecute, SQLRead, ExportToHDFS, LoadFromHDFS)

  override val info: String = "Component for interacting with postgres database"

}

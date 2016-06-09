package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigRenderOptions}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.task.settings.ConnectionProfile
import tech.artemisia.util.Util

/**
 * Created by chlr on 4/22/16.
 */

/**
 *
 * @param name name of the task
 * @param sql query to be executed
 * @param connectionProfile connection profile to use
 */
abstract class SQLRead(name: String = Util.getUUID, val sql: String, val connectionProfile: ConnectionProfile)
  extends Task(name) {

  val dbInterface: DBInterface

  override protected[task] def setup(): Unit = {}

  /**
   * execute query and parse as config file.
   * considers only the first row of the query
    *
    * @return config file object
   */
  override protected[task] def work(): Config = {
    val result = dbInterface.queryOne(sql)
    AppLogger debug s"query result ${result.root().render(ConfigRenderOptions.concise())}"
    result
  }

  override protected[task] def teardown(): Unit = {}

}

object SQLRead extends TaskLike {

  override val taskName = "SQLRead"

  override val info = "execute select queries and wraps the results in config"

  override def doc(component: String) =
    s"""| ${classOf[SQLRead].getSimpleName} task runs a select query and parse the first row as a Hocon Config.
        | The query must be select query and not any DML or DDL statements.
        | The configuration object is shown below.
        |
        | {
        |   Component = $component
        |   Task = ${classOf[SQLRead].getSimpleName}
        |     params = {
        |      dsn = ?
        |      [sql|sqlfile] = ?
        |    }
        | }
        |
        |Its param include
        |  dsn = either a name of the dsn or a config-object with username/password and other credentials
        |  sql = select query to be run
        |  sqlfile = the file containing the query
        |
    """.stripMargin

  override def apply(name: String, config: Config) = ???

}

package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigRenderOptions}
import tech.artemisia.core.AppLogger
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.Task
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.Util

import scala.reflect.ClassTag

/**
 * Created by chlr on 4/22/16.
 */

/**
 *
 * @param name name of the task
 * @param sql query to be executed
 * @param connectionProfile connection profile to use
 */
abstract class SQLRead(name: String = Util.getUUID, val sql: String, val connectionProfile: DBConnection)
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

  override protected[task] def teardown() = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
  }

}

object SQLRead {

  val taskName = "SQLRead"

  val info = "execute select queries and wraps the results in config"

  val desc =
    s"""
      |$taskName task runs a select query and parse the first row as a Hocon Config.
      |The query must be select query and not any DML or DDL statements.
      |The configuration object is shown below.
    """.stripMargin

  def configStructure(component: String): String = {
    s"""
       |{
       |  Component = $component
       |  Task = $taskName
       |    params = {
       |     dsn = ?
       |     [sql|sqlfile] = ?
       |   }
  """.stripMargin
  }


  val fieldDefinition = Seq(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "sql" -> "select query to be run",
    "sqlfile" -> "the file containing the query"
  )


  def create[T <: SQLRead: ClassTag](name: String, config: Config) = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[SQLRead]).getConstructor(classOf[String], classOf[String]
    , classOf[DBConnection]).newInstance(name, sql, connectionProfile)
  }

}

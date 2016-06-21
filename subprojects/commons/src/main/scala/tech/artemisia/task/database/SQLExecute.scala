package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag

/**
 * Created by chlr on 5/21/16.
 */

/**
 * An abstract task to execute a query
  *
  * @param name a name for this task
 * @param sql query to be executed
 * @param connectionProfile connection detail for the database
 */
abstract class SQLExecute(name: String, val sql: String, val connectionProfile: DBConnection) extends Task(name) {

  val dbInterface: DBInterface

  override protected[task] def setup(): Unit

  /**
   * The query is executed in this phase.
   * returns number of rows updated as a status node in config object
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
   */
  override protected[task] def work(): Config = {
    val updatedRows = dbInterface.execute(sql)
    AppLogger debug s"$updatedRows rows updated"
    wrapAsStats {
      ConfigFactory parseString
        s"""
          | updated = $updatedRows
        """.stripMargin
    }
  }

  override protected[task] def teardown(): Unit
}

object SQLExecute {

  val taskName = "SQLExecute"

  val info = "executes DML statements such as Insert/Update/Delete"

  val desc = s"$taskName task is used execute arbitary DML statements against a database"

  def configStructure(component: String) = {
    s"""
       |{
       |  Component = $component
       |  Task = $taskName
       |    params = {
       |     dsn = ?
       |     [sql|sqlfile] = ?
       |   }
       |}
     """.stripMargin
  }

  val fieldDefinition = Seq(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "sql" -> "select query to be run",
    "sqlfile" -> "the file containing the query"
  )


  def create[T <: SQLExecute: ClassTag](name: String, config: Config) = {
    val sql = config.as[String]("sql")
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
      implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[SQLExecute]).getConstructor(classOf[String],
        classOf[String], classOf[DBConnection]).newInstance(name, sql, connectionProfile)
  }
}



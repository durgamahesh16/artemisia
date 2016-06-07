package tech.artemisia.task.database

import com.typesafe.config.{ConfigFactory, Config}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.ConnectionProfile

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
abstract class SQLExecute(name: String, val sql: String, val connectionProfile: ConnectionProfile) extends Task(name) {

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

  /**
    * @return one line description of the task
    */
  def info = "executes DML statements such as Insert/Update/Delete"



  /**
    * brief description of the task
    */
  def doc(component: String) =
    s"""|  ${classOf[LoadToTable].getSimpleName} task is used execute arbitary DML statements against a database
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

}



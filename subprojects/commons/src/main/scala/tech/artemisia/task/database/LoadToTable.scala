package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{DBConnection, LoadSettings}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.DocStringProcessor._

import scala.reflect.ClassTag

/**
 * Created by chlr on 4/30/16.
 */

/**
 * An abstract task to load data into a table
 *
 * @param name name for the task
 * @param tableName destination table to be loaded
 * @param connectionProfile connection details for the database
 * @param loadSettings load setting details
 */
abstract class LoadToTable(name: String, val tableName: String, val connectionProfile: DBConnection,
                           val loadSettings: LoadSettings) extends Task(name) {

  val dbInterface: DBInterface

  /**
   * Actual data export is done in this phase.
   * Number of records loaded is emitted in stats node
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
   */
  override def work(): Config = {
    val (totalRows, rejectedCnt) = dbInterface.load(tableName, loadSettings)
    AppLogger info s"${totalRows - rejectedCnt} rows loaded into table $tableName"
    AppLogger info s"$rejectedCnt row were rejected"
    wrapAsStats {
      ConfigFactory parseString
        s"""
           |loaded = ${totalRows-rejectedCnt}
           |rejected = $rejectedCnt
         """.stripMargin
    }
  }

}

object LoadToTable {

  val taskName = "LoadToTable"

  val info = "load a file into a table"


  def doc(component: String, defaultPort: Int) =
    s"""
      | $taskName
      | ${"-" * taskName.length}
      |
      | $taskName task is used to load content into a table typically from a file.
      | the configuration object for this task is as shown below.
      |
      |```
      |     Component = $component
      |     Task = $taskName
      |     params = {
      |	             dsn = <% connection-name
      |                      <-------------------------------->
      |                      ${DBConnection.structure(defaultPort).ident(23)}
      |                     %>
      |	             destination-table = "dummy_table" @required
      |	             load-setting = ${LoadSettings.structure.ident(20)}
      |            }
      |```
      |
      |
      | field legends:
      |    * dsn:  either a name of the dsn or a config-object with username/password and other credentials
      |    * destination-table: destination table to load
      |    * loadsetting:
      |             ${LoadSettings.fieldDescription.ident(12)}
    """.stripMargin

  def create[T <: LoadToTable : ClassTag](name: String, config: Config): LoadToTable = {
      val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
      val destinationTable = config.as[String]("destination-table")
      val loadSettings = LoadSettings(config.as[Config]("load-setting"))
      implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[LoadToTable]).getConstructor(classOf[String],
        classOf[String], classOf[DBConnection], classOf[LoadSettings]).newInstance(name, destinationTable,
        connectionProfile, loadSettings)
  }

}

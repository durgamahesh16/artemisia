package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{ConnectionProfile, LoadSettings}

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
abstract class LoadToTable(name: String, val tableName: String, val connectionProfile: ConnectionProfile,
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

  /**
    * one line description of the task
    */
  val info = "load a file into a table"


  /**
    *
    * @param component name of the component
    * @return task documentation
    */
  def doc(component: String) =
    s"""
      | ${classOf[LoadToTable].getSimpleName} task is used to load content into a table typically from a file.
      | the configuration object for this task is as shown below.
      |
      | Component = $component
      | Task = ${classOf[LoadToTable].getSimpleName}
      | params = {
      |	  dsn =
      |	  destination-table = ""
      |	  load-setting = {
      |       load-path = ?
      |		    header =  no
      |		    skip-lines = 0
      |		    delimiter = ","
      |		    quoting = no
      |		    quotechar = "\""
      |	      escapechar = "\\"
      |	      mode = default
      |	      error-file = ?
      |	      error-tolerence = 2
      |	  },
      | }
      |
      | dsn =  either a name of the dsn or a config-object with username/password and other credentials
      | destination-table = destination table to load
      | load-setting =
      |     load-path = path to load from (eg: /var/tmp/input.txt)
      |     header = boolean field to enable/disable headers
      |     skip-lines = number of lines to skip in he table
      |     delimiter = delimiter of the file
      |     quoting = boolean field to indicate if the file is quoted.
      |     quotechar = character to be used for quoting
      |     escapechar = escape character used in the file
      |     mode = mode of loading the table
      |     error-file = location of the file where rejected error records are saved.
      |     error-tolerance = % of data that is allowable to get rejected
      |
    """.stripMargin

}

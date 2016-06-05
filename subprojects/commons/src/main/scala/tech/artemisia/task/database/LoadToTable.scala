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
abstract class LoadToTable(name: String, tableName: String, connectionProfile: ConnectionProfile,
                           loadSettings: LoadSettings) extends Task(name) {

  val dbInterface: DBInterface

  /**
   * Actual data export is done in this phase.
   * Number of records loaded is emitted in stats node
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
    * @return one line description of the task
    */
  def info = "load a file into a table"

}

package tech.artemisia.task.database

import java.io.InputStream
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{DBConnection, LoadSetting}

/**
 * Created by chlr on 4/30/16.
 */

/**
 * An abstract task to load data into a table
 *
 * @param name name for the task
 * @param tableName destination table to be loaded
 * @param connectionProfile connection details for the database
 * @param loadSetting load setting details
 */
abstract class LoadFromFile(val name: String, val tableName: String, val location: URI, val connectionProfile: DBConnection,
                            val loadSetting: LoadSetting) extends Task(name) {

  val dbInterface: DBInterface

  /**
    * inputStream to read data from
    */
  val source: Either[InputStream, URI]

  /**
    * list of supported modes
    */
  protected val supportedModes: Seq[String]

  override protected[task] def setup() = {
    require(supportedModes contains loadSetting.mode, s"unsupported mode ${loadSetting.mode}")
    if (loadSetting.truncate) {
      AppLogger info s"truncating table $tableName"
      dbInterface.execute(s"DELETE FROM $tableName",printSQL =  false)
    }
  }

  /**
   * Actual data export is done in this phase.
   * Number of records loaded is emitted in stats node
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
   */
  override def work(): Config = {
    val (totalRows, rejectedCnt) = dbInterface.loadTable(tableName, source, loadSetting)
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

   override protected[task] def teardown() = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
     source match {
       case Left(stream) => AppLogger debug s"closing InputStream from ${location.toString}"
                            stream.close()
       case Right(_) => ()
     }
   }
}


package tech.artemisia.task.database

import java.io.OutputStream
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{DBConnection, ExportSetting}

/**
  * Created by chlr on 4/13/16.
  */

/**
  *
  * @param name              name of the task instance
  * @param sql               query for the export
  * @param connectionProfile Connection Profile settings
  * @param exportSetting     Export settings
  */
abstract class ExportToFile(val name: String, val sql: String, val location: URI, val connectionProfile: DBConnection, val exportSetting: ExportSetting)
  extends Task(name: String) {

  val dbInterface: DBInterface

  val target: Either[OutputStream, URI]

  val supportedModes: Seq[String]

  override protected[task] def setup(): Unit = {}

  /**
    *
    * SQL export to file
    *
    * @return Config object with key rows and values as total number of rows exports
    */
  override protected[task] def work(): Config = {
    AppLogger info s"exporting data to ${location.toString}"
    val records = dbInterface.exportSQL(sql, target, exportSetting)
    AppLogger info s"exported $records rows to ${location.toString}"
    wrapAsStats {
      ConfigFactory parseString
        s"""
           | rows = $records
           """.stripMargin
    }
  }

  override protected[task] def teardown() = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
    target match {
      case Left(stream) => AppLogger debug s"closing OutputStream to ${location.toString}"
        stream.close()
      case Right(_) => ()
    }
  }

}






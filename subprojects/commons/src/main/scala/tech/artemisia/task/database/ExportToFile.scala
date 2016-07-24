package tech.artemisia.task.database

import java.io.{File, OutputStream}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag

/**
 * Created by chlr on 4/13/16.
 */

/**
  *
  * @param name name of the task instance
  * @param sql query for the export
  * @param connectionProfile Connection Profile settings
  * @param exportSettings Export settings
  */
abstract class ExportToFile(val name: String, val sql: String, val location: URI, val connectionProfile: DBConnection ,val exportSetting: ExportSetting)
  extends Task(name: String) {

     val dbInterface: DBInterface
     val target: Either[OutputStream, URI]

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

  object ExportToFile  {

  val taskName = "SQLExport"

  val info = "export query results to a file"

  val desc: String =
    s"""
       |$taskName task is used to export SQL query results to a file.
       |The typical task $taskName configuration is as shown below
     """.stripMargin

  val defaultConfig: Config = ConfigFactory.empty().withValue("export",BasicExportSetting.defaultConfig.root())

  def paramConfigDoc(defaultPort: Int) = {
    val config = ConfigFactory parseString
    s"""
       |{
       |   "dsn_[1]" = connection-name
       |   sql = "SELECT * FROM TABLE @optional(either sql or sqlfile key is required)"
       |   sqlfile = "run_queries.sql @info(path to the file) @optional(either sql or sqlfile key is required)"
       |   location = "/var/tmp/file.txt"
       |}
     """.stripMargin
    config
      .withValue(""""dsn_[2]"""",DBConnection.structure(defaultPort).root())
      .withValue("export",BasicExportSetting.structure.root())
  }

  val fieldDefinition: Map[String, AnyRef] = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "export" -> BasicExportSetting.fieldDescription,
    "location" -> "path to the target file"
  )

  /**
    *
    * @param name task name
    * @param config task configuration
    */
  def create[T <: ExportToFile : ClassTag](name: String, config: Config): ExportToFile = {
    val exportSettings = BasicExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val location = new File(config.as[String]("file")).toURI
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[URI], classOf[DBConnection],
      classOf[BasicExportSetting]).newInstance(name, sql, location, connectionProfile, exportSettings).asInstanceOf[ExportToFile]
  }

}




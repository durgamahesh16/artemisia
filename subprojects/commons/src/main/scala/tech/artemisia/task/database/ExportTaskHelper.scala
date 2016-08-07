package tech.artemisia.task.database

import java.io.File
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.TaskLike
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */


trait ExportTaskHelper extends TaskLike {

  val taskName = "SQLExport"

  val info = "export query results to a file"

  val desc: String =
    s"""
       |$taskName task is used to export SQL query results to a file.
       |The typical task $taskName configuration is as shown below
     """.stripMargin

  def supportedModes: Seq[String]

  override def defaultConfig: Config = ConfigFactory.empty().withValue("export",BasicExportSetting.defaultConfig.root())

  val defaultPort: Int

  override def paramConfigDoc = {
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

  override def fieldDefinition: Map[String, AnyRef] = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "export" -> BasicExportSetting.fieldDescription,
    "location" -> "path to the target file"
  )


}

object ExportTaskHelper {

  /**
    *
    * @param name task name
    * @param config task configuration
    */
  def create[T <: ExportToFile : ClassTag](name: String, config: Config): ExportToFile = {
    val exportSettings = BasicExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val location = new File(config.as[String]("file")).toURI
    val sql = config.asInlineOrFile("sql")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[URI], classOf[DBConnection],
      classOf[BasicExportSetting]).newInstance(name, sql, location, connectionProfile, exportSettings).asInstanceOf[ExportToFile]
  }

}

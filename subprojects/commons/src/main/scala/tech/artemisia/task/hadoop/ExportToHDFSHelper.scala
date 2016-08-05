package tech.artemisia.task.hadoop

import com.typesafe.config.Config
import tech.artemisia.task.database.{BasicExportSetting, ExportTaskHelper}
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */

trait ExportToHDFSHelper extends ExportTaskHelper {

  override val taskName: String = "ExportToHDFS"

  override def paramConfigDoc = super.paramConfigDoc
                      .withValue("hdfs", HDFSWriteSetting.structure.root())
                      .withoutPath("location")

  override def defaultConfig: Config = super.defaultConfig.withValue("hdfs", HDFSWriteSetting.defaultConfig.root())

  override def fieldDefinition: Map[String, AnyRef] = super.fieldDefinition - "location" +
                                            ("hdfs" -> HDFSWriteSetting.fieldDescription)

  override val info: String = "Export database resultset to HDFS"

  override val desc: String = ""

}

object ExportToHDFSHelper {


  def create[T <: ExportToHDFS: ClassTag](name: String, config: Config): T = {
    val exportSettings = BasicExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val hdfs = HDFSWriteSetting(config.as[Config]("hdfs"))
    val sql: String = config.asInlineOrFile("key")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[HDFSWriteSetting],
      classOf[DBConnection], classOf[ExportSetting])
      .newInstance(name, sql, hdfs, connectionProfile, exportSettings).asInstanceOf[T]
  }

}
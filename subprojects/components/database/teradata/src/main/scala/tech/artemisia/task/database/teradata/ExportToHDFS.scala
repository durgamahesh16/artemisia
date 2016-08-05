package tech.artemisia.task.database.teradata

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.{ExportToHDFSHelper, HDFSWriteSetting}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, hadoop}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 7/22/16.
  */

class ExportToHDFS(override val taskName: String, override val sql: String, override  val hdfsWriteSetting: HDFSWriteSetting,
                   override val connectionProfile: DBConnection, override val exportSetting: TeraExportSetting)
    extends hadoop.ExportToHDFS(taskName, sql, hdfsWriteSetting, connectionProfile, exportSetting) {

   override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, exportSetting.mode)

   override val supportedModes: Seq[String] = ExportToHDFS.supportedModes
}

object ExportToHDFS extends ExportToHDFSHelper {

  override def apply(name: String, config: Config): Task = {
    val exportSetting = TeraExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val hdfs = HDFSWriteSetting(config.as[Config]("hdfs"))
    val sql: String = config.asInlineOrFile("sql")
    new ExportToHDFS(taskName, sql, hdfs, connectionProfile, exportSetting)
  }

  override def paramConfigDoc: Config = super.paramConfigDoc
                                        .withValue("export", TeraExportSetting.structure.root())

  override def defaultConfig: Config = super.defaultConfig
                                        .withValue("export", TeraExportSetting.defaultConfig.root())

  override def fieldDefinition: Map[String, AnyRef] = super.fieldDefinition +
                                                    ("export" -> TeraExportSetting.fieldDescription)

  override def supportedModes: Seq[String] = "default" :: "fastexport" :: Nil

  override val defaultPort: Int = 1025

}

package tech.artemisia.task.database.teradata

import com.typesafe.config.Config
import tech.artemisia.task.database.{DBInterface, ExportTaskHelper}
import tech.artemisia.task.hadoop.{ExportToHDFSHelper, HDFSWriteSetting}
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.task.{Task, hadoop}

/**
  * Created by chlr on 7/22/16.
  */

class ExportToHDFS(override val taskName: String, override val sql: String, override  val hdfsWriteSetting: HDFSWriteSetting,
                   override val connectionProfile: DBConnection, override val exportSetting: ExportSetting)
    extends hadoop.ExportToHDFS(taskName, sql, hdfsWriteSetting, connectionProfile, exportSetting) {

   override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, exportSetting.mode)

   override val supportedModes: Seq[String] = ExportToHDFS.supportedModes
}

object ExportToHDFS extends ExportToHDFSHelper {

  override def apply(name: String, config: Config): Task = ExportTaskHelper.create[ExportToHDFS](name, config)

  override def supportedModes: Seq[String] = "default" :: "fastexport" :: Nil

  override val defaultPort: Int = 1025

}

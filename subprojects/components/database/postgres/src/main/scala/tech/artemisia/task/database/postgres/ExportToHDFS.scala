package tech.artemisia.task.database.postgres

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.{ExportToHDFSHelper, HDFSWriteSetting}
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.task.{Task, hadoop}

/**
  * Created by chlr on 7/22/16.
  */
class ExportToHDFS(taskName: String, sql: String, hdfsWriteSetting: HDFSWriteSetting,
                   connectionProfile: DBConnection, exportSetting: ExportSetting)
      extends hadoop.ExportToHDFS(taskName, sql, hdfsWriteSetting, connectionProfile, exportSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, exportSetting.mode)

  override val supportedModes: Seq[String] = ExportToHDFS.supportedModes

}

object ExportToHDFS extends ExportToHDFSHelper {

  override def apply(name: String, config: Config): Task = ExportToHDFSHelper.create[ExportToHDFS](name, config)

  override def supportedModes: Seq[String] = "default" :: "bulk" :: Nil

  override val defaultPort: Int = 5432

}

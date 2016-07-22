package tech.artemisia.task.database.postgres

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.HDFSWriteSetting
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.task.{Task, TaskLike, hadoop}

/**
  * Created by chlr on 7/22/16.
  */
class ExportToHDFS(taskName: String, sql: String, hdfsWriteSetting: HDFSWriteSetting,
                   connectionProfile: DBConnection, exportSetting: ExportSetting)
      extends hadoop.ExportToHDFS(taskName, sql, hdfsWriteSetting, connectionProfile, exportSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, exportSetting.mode)

}

object ExportToHDFS extends TaskLike {

  override val taskName: String = hadoop.ExportToHDFS.taskName

  override def apply(name: String, config: Config): Task = {
    hadoop.ExportToHDFS.create[ExportToHDFS](name, config)
  }

  override val paramConfigDoc: Config = hadoop.ExportToHDFS.paramConfigDoc(5432)
  override val defaultConfig: Config = hadoop.ExportToHDFS.defaultConfig
  override val fieldDefinition: Map[String, AnyRef] = hadoop.ExportToHDFS.fieldDefinition
  override val info: String = hadoop.ExportToHDFS.info
  override val desc: String = hadoop.ExportToHDFS.desc

}

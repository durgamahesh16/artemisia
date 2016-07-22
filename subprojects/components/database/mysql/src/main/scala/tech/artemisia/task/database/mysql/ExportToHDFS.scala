package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.HDFSWriteSetting
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.task.{Task, TaskLike, hadoop}

/**
  * Created by chlr on 7/21/16.
  */


class ExportToHDFS(override val taskName: String, override val sql: String, hdfsWriteSetting: HDFSWriteSetting,
                   override val connectionProfile: DBConnection, override val exportSetting: ExportSetting)
      extends  hadoop.ExportToHDFS(taskName, sql, hdfsWriteSetting, connectionProfile, exportSetting) {

      override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, exportSetting.mode)
 }

object ExportToHDFS extends TaskLike {

  override val taskName: String = "ExportToHDFS"

  override def apply(name: String, config: Config): Task = {
    hadoop.ExportToHDFS.create[ExportToHDFS](name, config)
  }

  override val paramConfigDoc: Config = hadoop.ExportToHDFS.paramConfigDoc(3306)

  override val defaultConfig: Config = hadoop.ExportToHDFS.defaultConfig

  override val fieldDefinition: Map[String, AnyRef] = hadoop.ExportToHDFS.fieldDefinition

  override val info: String = hadoop.ExportToHDFS.info

  override val desc: String = hadoop.ExportToHDFS.desc

}

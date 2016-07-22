package tech.artemisia.task.database.teradata

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.HDFSWriteSetting
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskLike, hadoop}

/**
  * Created by chlr on 7/22/16.
  */

class ExportToHDFS(override val taskName: String, override val sql: String, override  val hdfsWriteSetting: HDFSWriteSetting,
                   override val connectionProfile: DBConnection, override val exportSetting: TeraExportSetting)
    extends hadoop.ExportToHDFS(taskName, sql, hdfsWriteSetting, connectionProfile, exportSetting) {

   override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, exportSetting.mode)

}

object ExportToHDFS extends TaskLike {

  override val taskName: String = hadoop.ExportToHDFS.taskName

  override def apply(name: String, config: Config): Task = {
    hadoop.ExportToHDFS.create[ExportToHDFS](name, config)
  }

  override val paramConfigDoc: Config = hadoop.ExportToHDFS.paramConfigDoc(1025)

  override val defaultConfig: Config = hadoop.ExportToHDFS.defaultConfig

  override val fieldDefinition: Map[String, AnyRef] = hadoop.ExportToHDFS.fieldDefinition

  override val info: String = hadoop.ExportToHDFS.info

  override val desc: String = hadoop.ExportToHDFS.desc

}

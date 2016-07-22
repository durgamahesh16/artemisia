package tech.artemisia.task.database.postgres

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.HDFSReadSetting
import tech.artemisia.task.{Task, TaskLike, hadoop}
import tech.artemisia.task.settings.{DBConnection, LoadSetting}

/**
  * Created by chlr on 7/22/16.
  */

class LoadFromHDFS(taskName: String, tableName: String, hdfsReadSetting: HDFSReadSetting, connectionProfile: DBConnection,
                   loadSetting: LoadSetting) extends hadoop.LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, loadSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

}

object LoadFromHDFS extends TaskLike {

  override val taskName: String = hadoop.LoadFromHDFS.taskName

  override def apply(name: String, config: Config): Task = {
    hadoop.LoadFromHDFS.create[LoadFromHDFS](name, config)
  }

  override val paramConfigDoc: Config = hadoop.LoadFromHDFS.paramConfigDoc(5432)
  override val defaultConfig: Config = hadoop.LoadFromHDFS.defaultConfig
  override val fieldDefinition: Map[String, AnyRef] = hadoop.LoadFromHDFS.fieldDefinition
  override val info: String = hadoop.LoadFromHDFS.info
  override val desc: String = hadoop.LoadFromHDFS.desc

}

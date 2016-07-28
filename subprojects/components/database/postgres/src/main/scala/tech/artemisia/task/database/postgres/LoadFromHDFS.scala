package tech.artemisia.task.database.postgres

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.{HDFSReadSetting, LoadFromHDFSHelper}
import tech.artemisia.task.settings.{DBConnection, LoadSetting}
import tech.artemisia.task.{Task, hadoop}

/**
  * Created by chlr on 7/22/16.
  */

class LoadFromHDFS(taskName: String, tableName: String, hdfsReadSetting: HDFSReadSetting, connectionProfile: DBConnection,
                   loadSetting: LoadSetting) extends hadoop.LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, loadSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

  override protected val supportedModes: Seq[String] = LoadFromHDFS.supportedModes

}

object LoadFromHDFS extends LoadFromHDFSHelper {

  override def apply(name: String, config: Config): Task = LoadFromHDFSHelper.create[LoadFromHDFS](name, config)

  override def defaultPort = 3306

  override def supportedModes = "default" :: "bulk" :: Nil

}

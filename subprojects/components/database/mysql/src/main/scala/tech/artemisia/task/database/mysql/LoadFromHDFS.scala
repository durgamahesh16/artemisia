package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.{HDFSReadSetting, LoadFromHDFSHelper}
import tech.artemisia.task.settings.{DBConnection, LoadSetting}
import tech.artemisia.task.{Task, hadoop}

/**
  * Created by chlr on 7/21/16.
  */
class LoadFromHDFS(override val taskName: String, override val tableName: String, override val hdfsReadSetting: HDFSReadSetting,
                   override val  connectionProfile: DBConnection, override val loadSetting: LoadSetting)
 extends hadoop.LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, loadSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

  override val supportedModes: Seq[String] = LoadFromHDFS.supportedModes

}

object LoadFromHDFS extends LoadFromHDFSHelper {


  override def apply(name: String, config: Config): Task = LoadFromHDFSHelper.create[LoadFromHDFS](name, config)

  override def defaultPort = 3306

  override val supportedModes = "default" :: "bulk" :: Nil

}

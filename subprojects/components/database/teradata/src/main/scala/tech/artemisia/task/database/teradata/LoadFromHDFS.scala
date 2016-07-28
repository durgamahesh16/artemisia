package tech.artemisia.task.database.teradata

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.{HDFSReadSetting, LoadFromHDFSHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, hadoop}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 7/22/16.
  */
class LoadFromHDFS(override val taskName: String, override val tableName: String, override val hdfsReadSetting: HDFSReadSetting,
                   override val connectionProfile: DBConnection, override val loadSetting: TeraLoadSetting) extends
  hadoop.LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, loadSetting) {

  override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

  override protected val supportedModes: Seq[String] = LoadFromHDFS.supportedModes
}

object LoadFromHDFS extends LoadFromHDFSHelper {

  override def defaultPort: Int = 1025

  override def apply(name: String, config: Config): Task = {
    val loadSetting = TeraLoadSetting(config.as[Config]("load"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val tableName = config.as[String]("destination-table")
    val hdfsReadSetting = HDFSReadSetting(config.as[Config]("hdfs"))
    new LoadFromHDFS(name, tableName, hdfsReadSetting, connectionProfile, loadSetting )
  }

  override def paramConfigDoc: Config = super.paramConfigDoc
                                    .withValue("load", TeraLoadSetting.structure.root())

  override val defaultConfig: Config = super.defaultConfig
                                    .withValue("load", TeraLoadSetting.defaultConfig.root())

  override val fieldDefinition: Map[String, AnyRef] = super.fieldDefinition +
                                    ("load" -> TeraLoadSetting.fieldDescription)

  override def supportedModes = "default" :: "fastload" :: Nil

}

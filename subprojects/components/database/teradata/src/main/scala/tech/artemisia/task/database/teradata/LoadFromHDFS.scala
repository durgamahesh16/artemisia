package tech.artemisia.task.database.teradata

import java.io.InputStream
import java.net.URI

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.hadoop.{HDFSReadSetting, LoadFromHDFSHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, hadoop}
import tech.artemisia.util.HoconConfigUtil.Handler


/**
  * Task to load a teradata table from hdfs. This is abstract class which cannot be directly constructed.
  * use the apply method to construct the task.
  *
  * @param taskName task name
  * @param tableName target table
  * @param hdfsReadSetting HDFS read setting
  * @param connectionProfile database connection profile
  * @param loadSetting load setting
  */
abstract class LoadFromHDFS(override val taskName: String, override val tableName: String, override val hdfsReadSetting: HDFSReadSetting,
                   override val connectionProfile: DBConnection, override val loadSetting: TeraLoadSetting) extends
  hadoop.LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, loadSetting) {

  override implicit val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

  override val supportedModes: Seq[String] = LoadFromHDFS.supportedModes

  override val source: Either[InputStream, URI]

  /**
    * No operations are done in this phase
    */
  override protected[task] def setup(): Unit = {
    if (loadSetting.truncate) {
      TeraUtils.truncateElseDrop(tableName)
    }
  }

}

object LoadFromHDFS extends LoadFromHDFSHelper {

  override def defaultPort: Int = 1025

  override def apply(name: String, config: Config): Task = {
    val loadSetting = TeraLoadSetting(config.as[Config]("load"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val tableName = config.as[String]("destination-table")
    val hdfsReadSetting = HDFSReadSetting(config.as[Config]("hdfs"))
    LoadFromHDFS(name, tableName, hdfsReadSetting, connectionProfile, loadSetting)
  }

  def apply(taskName: String, tableName: String, hdfsReadSetting: HDFSReadSetting,connectionProfile: DBConnection,
  loadSetting: TeraLoadSetting) = {
    lazy val (hdfsStream: InputStream, loadSize: Long) = hadoop.LoadFromHDFS.getPathForLoad(hdfsReadSetting)
   val normalizedLoadSetting = TeraUtils.autoTuneLoadSettings(loadSize, loadSetting)
    new LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, normalizedLoadSetting) {
      override lazy val source = Left(hdfsStream)
    }
  }

  override def paramConfigDoc: Config = super.paramConfigDoc
                                    .withValue("load", TeraLoadSetting.structure.root())

  override val defaultConfig: Config = super.defaultConfig
                                    .withValue("load", TeraLoadSetting.defaultConfig.root())

  override val fieldDefinition: Map[String, AnyRef] = super.fieldDefinition +
                                    ("load" -> TeraLoadSetting.fieldDescription)

  override def supportedModes = "default" :: "fastload" :: "auto" :: Nil

}

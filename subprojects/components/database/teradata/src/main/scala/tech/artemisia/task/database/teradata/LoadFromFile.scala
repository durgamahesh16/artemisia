package tech.artemisia.task.database.teradata

import java.net.URI
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database
import tech.artemisia.task.database.{DBInterface, LoadTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.{FileSystemUtil, Util}

/**
  * Created by chlr on 6/26/16.
  */

/**
  * Task to load a teradata table from local filesystem. This is abstract class which cannot be directly constructed.
  * use the apply method to construct the task.
  *
  * @param taskName taskname
  * @param tableName destination table to be loaded
  * @param location path to load from
  * @param connectionProfile database connection profile
  * @param loadSetting load setting details
  */
abstract class LoadFromFile(override val taskName: String, override val tableName: String,
                            location: URI, override val connectionProfile: DBConnection, override val loadSetting: TeraLoadSetting)
  extends database.LoadFromFile(taskName, tableName, location, connectionProfile, loadSetting) {

  override val supportedModes = LoadFromFile.supportedModes

  override implicit val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

  /**
    * No operations are done in this phase
    */
  override protected[task] def setup(): Unit = {
    if (loadSetting.recreateTable) {
       TeraUtils.dropRecreateTable(tableName)
    } else {
      super.setup()
    }
  }

  /**
    * No operations are done in this phase
    */
  override protected[task] def teardown(): Unit = {}

}

object LoadFromFile extends LoadTaskHelper {


  override val defaultConfig = ConfigFactory.empty().withValue("load", TeraLoadSetting.defaultConfig.root())

  override def apply(name: String, config: Config) = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = TeraLoadSetting(config.as[Config]("load"))
    val location = new URI(config.as[String]("location"))
    LoadFromFile(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  def apply(taskName: String = Util.getUUID, tableName: String, location: URI, connectionProfile: DBConnection,
            loadSetting: TeraLoadSetting) = {
    lazy val (inputStream, loadSize) = FileSystemUtil.getPathForLoad(Paths.get(location.getPath))
    val normalizedLoadSettings = TeraUtils.autoTuneLoadSettings(loadSize,loadSetting)
    new LoadFromFile(taskName, tableName, location ,connectionProfile, normalizedLoadSettings) {
      override lazy val source = Left(inputStream)
    }
  }

  override def defaultPort = 1025

  override val paramConfigDoc =  super.paramConfigDoc.withValue("load",TeraLoadSetting.structure.root())

  override val fieldDefinition = super.fieldDefinition ++ Map("load" -> TeraLoadSetting.fieldDescription )

  override def supportedModes: Seq[String] = "fastload" :: "default" :: "auto" :: Nil

}


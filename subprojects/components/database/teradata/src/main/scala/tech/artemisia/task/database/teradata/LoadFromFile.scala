package tech.artemisia.task.database.teradata

import java.io.InputStream
import java.net.URI
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger._
import tech.artemisia.task.database
import tech.artemisia.task.database.{DBInterface, LoadTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/26/16.
  */

class LoadFromFile(override val taskName: String = Util.getUUID, override val tableName: String,
                   location: URI, override val connectionProfile: DBConnection, override val loadSetting: TeraLoadSetting)
  extends database.LoadFromFile(taskName, tableName, location, connectionProfile, loadSetting) {

  override val supportedModes = "fastload" :: "default" :: "auto" :: Nil

  val (inputStream, loadSize) =  getPathForLoad(Paths.get(location.getPath))

  override implicit val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, loadSetting.mode match {
      case "auto" => if (loadSize  > loadSetting.bulkLoadThreshold) "fastload" else "default"
      case x => x
    }
  )

  override lazy val source: Either[InputStream, URI] = Left(inputStream)

  /**
    * No operations are done in this phase
    */
  override protected[task] def setup(): Unit = {
    if (loadSetting.recreateTable) {
       TeraUtils.dropRecreateTable(tableName)
    } else {
        info("not dropping and recreating the table")
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
    new LoadFromFile(name, destinationTable, new URI(config.as[String]("location")) ,connectionProfile, loadSettings)
  }

  override def defaultPort = 1025

  override val paramConfigDoc =  super.paramConfigDoc.withValue("load",TeraLoadSetting.structure.root())

  override val fieldDefinition = super.fieldDefinition ++ Map("load" -> TeraLoadSetting.fieldDescription )

  override def supportedModes: Seq[String] = "fastload" :: "default" :: Nil

}


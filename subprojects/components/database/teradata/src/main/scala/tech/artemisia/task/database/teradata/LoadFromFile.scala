package tech.artemisia.task.database.teradata

import java.io.InputStream
import java.net.URI
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import tech.artemisia.task.database
import tech.artemisia.task.database.{DBInterface, LoadTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.Util

import scala.collection.mutable

/**
  * Created by chlr on 6/26/16.
  */

class LoadFromFile(override val taskName: String = Util.getUUID, override val tableName: String,
                   location: URI, override val connectionProfile: DBConnection, override val loadSetting: TeraLoadSetting)
  extends database.LoadFromFile(taskName, tableName, location, connectionProfile, loadSetting) {

  override val supportedModes = "fastload" :: "default" :: "auto" :: Nil

  val (inputStream, loadSize) =  prepPathForLoad(Paths.get(location.getPath))

  override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, loadSetting.mode match  {
      case "auto" => if (loadSize  > loadSetting.bulkLoadThreshold) "fastload" else "default"
      case x => x
    }
  )

  override val source: Either[InputStream, URI] = Left(inputStream)

  /**
    * No operations are done in this phase
    */
  override protected[task] def setup(): Unit = {
    if (loadSetting.recreateTable) {
        require(supportedModes contains loadSetting.mode, s"load mode ${loadSetting.mode} is not supported")
        val rs = dbInterface.query(s"SHOW TABLE $tableName", printSQL = false)
        val buffer = mutable.ArrayBuffer[String]()
        while(rs.next()) { buffer += rs.getString(1) }
        dbInterface.query(s"DROP TABLE $tableName", printSQL = true)
        dbInterface.query(buffer.mkString("\n"))
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
    val mutatedConfig = config withFallback ConfigFactory.empty().withValue("load.batch-size", ConfigValueFactory fromAnyRef {
      config.getString("load.mode").toLowerCase match {
        case "fastload" => 80000
        case "default" => 100
      }
    })
    val connectionProfile = DBConnection.parseConnectionProfile(mutatedConfig.getValue("dsn"))
    val destinationTable = mutatedConfig.as[String]("destination-table")
    val loadSettings = TeraLoadSetting(mutatedConfig.as[Config]("load"))
    new LoadFromFile(name, destinationTable, new URI(config.as[String]("location")) ,connectionProfile, loadSettings)
  }

  override def defaultPort = 1025

  override val paramConfigDoc =  super.paramConfigDoc.withValue("load",TeraLoadSetting.structure.root())

  override val fieldDefinition = super.fieldDefinition ++ Map("load" -> TeraLoadSetting.fieldDescription )

  override def supportedModes: Seq[String] = "fastload" :: "default" :: Nil

}


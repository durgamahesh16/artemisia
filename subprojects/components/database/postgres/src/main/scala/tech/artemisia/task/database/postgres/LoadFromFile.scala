package tech.artemisia.task.database.postgres

import java.io.InputStream
import java.net.URI
import java.nio.file.Paths
import com.typesafe.config.Config
import tech.artemisia.task.database
import tech.artemisia.task.database.{BasicLoadSetting, DBInterface, LoadTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */


class LoadFromFile(name: String = Util.getUUID, tableName: String, location: URI, connectionProfile: DBConnection, loadSettings: BasicLoadSetting)
  extends database.LoadFromFile(name, tableName, location, connectionProfile, loadSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSettings.mode)

  override protected val supportedModes = LoadFromFile.supportedModes

  override val source: Either[InputStream, URI] = loadSettings.mode match {
    case "default" => Left(prepPathForLoad(Paths.get(location.getPath))._1)
    case "bulk" => Right(location)
  }

  override protected[task] def setup(): Unit = {}

  override protected[task] def teardown(): Unit = {}

}

object LoadFromFile extends LoadTaskHelper {

  override def apply(name: String, config: Config) = LoadTaskHelper.create[LoadFromFile](name, config)

  override def defaultPort = 3306

  override val supportedModes: Seq[String] = "default" :: "bulk" :: Nil

}

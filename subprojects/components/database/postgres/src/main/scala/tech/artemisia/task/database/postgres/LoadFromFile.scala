package tech.artemisia.task.database.postgres

import java.io.InputStream
import java.net.URI
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database.{BasicLoadSetting, DBInterface}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */


class LoadFromFile(name: String = Util.getUUID, tableName: String, location: URI, connectionProfile: DBConnection, loadSettings: BasicLoadSetting)
  extends database.LoadFromFile(name, tableName, location, connectionProfile, loadSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSettings.mode)

  override protected val supportedModes = "default" :: "bulk" :: Nil

  override val source: Either[InputStream, URI] = loadSettings.mode match {
    case "default" => Left(prepPathForLoad(Paths.get(location.getPath))._1)
    case "bulk" => Right(location)
  }

  override protected[task] def setup(): Unit = {}

  override protected[task] def teardown(): Unit = {}

}

object LoadFromFile extends TaskLike {

  override val info = database.LoadFromFile.info

  override val defaultConfig: Config = ConfigFactory.empty()
            .withValue("load", BasicLoadSetting.defaultConfig.root())

  override val taskName = database.LoadFromFile.taskName

  override def apply(name: String, config: Config) = database.LoadFromFile.create[LoadFromFile](name, config)

  override val desc: String = database.LoadFromFile.desc

  override val paramConfigDoc = database.LoadFromFile.paramConfigDoc(5432)

  override val fieldDefinition = database.LoadFromFile.fieldDefinition

}

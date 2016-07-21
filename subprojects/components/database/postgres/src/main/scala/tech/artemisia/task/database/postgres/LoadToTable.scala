package tech.artemisia.task.database.postgres

import java.io.{File, FileInputStream, InputStream}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{BasicLoadSetting, DBConnection}
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */


class LoadToTable(name: String = Util.getUUID, tableName: String, location: URI, connectionProfile: DBConnection, loadSettings: BasicLoadSetting)
  extends database.LoadToTable(name, tableName, location, connectionProfile, loadSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSettings.mode)

  override val source: Either[InputStream, URI] = loadSettings.mode match {
    case "default" => Left(new FileInputStream(new File(location)))
    case "bulk" => Right(location)
  }

  override protected[task] def setup(): Unit = {}

  override protected[task] def teardown(): Unit = {}

}

object LoadToTable extends TaskLike {

  override val info = database.LoadToTable.info

  override val defaultConfig: Config = ConfigFactory.empty()
            .withValue("load-setting", BasicLoadSetting.defaultConfig.root())

  override val taskName = database.LoadToTable.taskName

  override def apply(name: String, config: Config) = database.LoadToTable.create[LoadToTable](name, config)

  override val desc: String = database.LoadToTable.desc

  override val paramConfigDoc = database.LoadToTable.paramConfigDoc(5432)

  override val fieldDefinition = database.LoadToTable.fieldDefinition

}

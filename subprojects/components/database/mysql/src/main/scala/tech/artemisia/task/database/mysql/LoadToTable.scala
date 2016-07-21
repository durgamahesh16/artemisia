package tech.artemisia.task.database.mysql

import java.io.{File, FileInputStream}
import java.net.URI
import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{BasicLoadSetting, DBConnection}
import tech.artemisia.util.Util

/**
 * Created by chlr on 4/30/16.
 */
class LoadToTable(name: String = Util.getUUID, tableName: String, location: URI, connectionProfile: DBConnection, loadSettings: BasicLoadSetting)
  extends database.LoadToTable(name, tableName, location, connectionProfile, loadSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSettings.mode)

  override val source = loadSettings.mode match {
    case "default" => Left(new FileInputStream(new File(location)))
    case "bulk" => Right(location)
  }

  /**
   * No operations are done in this phase
   */
  override protected[task] def setup(): Unit = {}

  /**
   * No operations are done in this phase
   */
  override protected[task] def teardown(): Unit = {}


}

object LoadToTable extends TaskLike {

  override val info = database.LoadToTable.info

  override val defaultConfig: Config = ConfigFactory.empty()
                      .withValue("load-setting", BasicLoadSetting.defaultConfig.root())

  override val taskName = database.LoadToTable.taskName

  override def apply(name: String, config: Config) = database.LoadToTable.create[LoadToTable](name, config)

  override val desc: String =  database.LoadToTable.desc

  override val paramConfigDoc =  database.LoadToTable.paramConfigDoc(3306)

  override val fieldDefinition = database.LoadToTable.fieldDefinition

}

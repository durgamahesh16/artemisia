package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/26/16.
  */

class LoadToTable(override val taskName: String = Util.getUUID, override val tableName: String,
                  override val connectionProfile: DBConnection, override val loadSettings: TeraLoadSetting)
  extends database.LoadToTable(taskName, tableName, connectionProfile, loadSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSettings.mode)

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

  override val taskName = database.LoadToTable.taskName

  override def apply(name: String, config: Config) = {
    val mutatedConfig = config withFallback ConfigFactory.empty().withValue("load-setting.batch-size", ConfigValueFactory fromAnyRef {
      config.getString("load-setting.mode").toLowerCase match {
        case "fastload" => 80000
        case "default" => 100
      }
    })
    val connectionProfile = DBConnection.parseConnectionProfile(mutatedConfig.getValue("dsn"))
    val destinationTable = mutatedConfig.as[String]("destination-table")
    val loadSettings = TeraLoadSetting(mutatedConfig.as[Config]("load-setting"))
    new LoadToTable(name, destinationTable, connectionProfile, loadSettings)
  }


  override val desc: String = database.LoadToTable.desc

  override val paramConfigDoc = database.LoadToTable.paramConfigDoc(1025)

  override val fieldDefinition = database.LoadToTable.fieldDefinition

}


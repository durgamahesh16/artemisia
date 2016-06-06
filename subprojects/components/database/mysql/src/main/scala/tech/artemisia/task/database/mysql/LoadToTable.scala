package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{ConnectionProfile, LoadSettings}
import tech.artemisia.util.Util
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/30/16.
 */
class LoadToTable(name: String = Util.getUUID, tablename: String, connectionProfile: ConnectionProfile, loadSettings: LoadSettings)
  extends tech.artemisia.task.database.LoadToTable(name, tablename, connectionProfile, loadSettings) {

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

object LoadToTable {

  def apply(name: String, config: Config): LoadToTable = {
    val connectionProfile = ConnectionProfile.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = LoadSettings(config.as[Config]("load-setting"))
    new LoadToTable(name, destinationTable, connectionProfile, loadSettings)
  }

  /**
    * @return one line description of the task
    */
  def info = tech.artemisia.task.database.LoadToTable.info


  /**
    * brief description of the task
    */
  val doc = tech.artemisia.task.database.LoadToTable.doc(classOf[MySQLComponent].getSimpleName)

}

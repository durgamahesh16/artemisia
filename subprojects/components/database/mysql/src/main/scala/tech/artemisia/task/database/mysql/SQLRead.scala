package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.ConnectionProfile
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.Util

class SQLRead(name: String = Util.getUUID, sql: String, connectionProfile: ConnectionProfile)
  extends tech.artemisia.task.database.SQLRead(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

}

object SQLRead {


  /**
   *
   * @param name task name
   * @param config configuration for the task
   * @return ExportToFile object
   */
  def apply(name: String, config: Config) = {
    val connectionProfile = ConnectionProfile.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    new SQLRead(name,sql,connectionProfile)
  }

  /**
    * @return one line description of the task
    */
  def info = tech.artemisia.task.database.SQLRead.info


  /**
    * brief description of the task
    */
  val doc = tech.artemisia.task.database.SQLRead.doc(classOf[MySQLComponent].getSimpleName)


}
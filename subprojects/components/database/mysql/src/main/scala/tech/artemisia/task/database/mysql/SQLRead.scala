package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.TaskLike
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.ConnectionProfile
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.Util

class SQLRead(name: String = Util.getUUID, sql: String, connectionProfile: ConnectionProfile)
  extends tech.artemisia.task.database.SQLRead(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

}

object SQLRead extends TaskLike {

  override val taskName = tech.artemisia.task.database.SQLRead.taskName

  override val info = tech.artemisia.task.database.SQLRead.info

  override def doc(component: String) = tech.artemisia.task.database.SQLRead.doc(component)

  override def apply(name: String, config: Config) = {
    val connectionProfile = ConnectionProfile.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    new SQLRead(name,sql,connectionProfile)
  }


}
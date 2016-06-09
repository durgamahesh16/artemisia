package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.TaskLike
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{ConnectionProfile, ExportSetting}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/22/16.
 */

class ExportToFile(name: String, sql: String, connectionProfile: ConnectionProfile ,exportSettings: ExportSetting)
  extends tech.artemisia.task.database.ExportToFile(name: String, sql: String, connectionProfile: ConnectionProfile ,exportSettings: ExportSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

  override protected[task] def setup(): Unit = {
    assert(exportSettings.file.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

}

object ExportToFile extends TaskLike {

  override val taskName = tech.artemisia.task.database.ExportToFile.taskName

  override def apply(name: String,config: Config) = {
    val exportSettings = ExportSetting(config.as[Config]("export"))
    val connectionProfile = ConnectionProfile.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    new ExportToFile(name,sql,connectionProfile,exportSettings)
  }

  override val info: String = tech.artemisia.task.database.ExportToFile.info

  override def doc(component: String): String = tech.artemisia.task.database.ExportToFile.doc(component)

}



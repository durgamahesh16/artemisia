package tech.artemisia.task.database.teradata

/**
  * Created by chlr on 6/26/16.
  */


import com.typesafe.config.Config
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 6/10/16.
  */


class ExportToFile(name: String, sql: String, connectionProfile: DBConnection ,exportSettings: ExportSetting)
  extends database.ExportToFile(name: String, sql: String, connectionProfile: DBConnection ,exportSettings: ExportSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, mode = exportSettings.mode, exportSettings.session)

  override protected[task] def setup(): Unit = {
    assert(exportSettings.file.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

}

object ExportToFile extends TaskLike {

  override val taskName = database.ExportToFile.taskName

  override def apply(name: String,config: Config) = {
    val exportSettings =  ExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    new ExportToFile(name, sql , connectionProfile, exportSettings)
  }

  override val info: String = database.ExportToFile.info

  override val desc: String = database.ExportToFile.desc

  override def configStructure(component: String): String = database.ExportToFile.configStructure(component, 1025)

  override val fieldDefinition = database.ExportToFile.fieldDefinition
}


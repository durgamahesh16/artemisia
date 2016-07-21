package tech.artemisia.task.database.teradata

/**
  * Created by chlr on 6/26/16.
  */

import java.io.{File, FileOutputStream, OutputStream}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.FileSystemUtil
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 6/10/16.
  */


class ExportToFile(override val name: String, override val sql: String, location: URI, override val connectionProfile: DBConnection
                   ,override val exportSettings: TeraExportSetting)
  extends database.ExportToFile(name: String, sql: String, location, connectionProfile: DBConnection ,exportSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, mode = exportSettings.mode,
    exportSettings.session)

  override val target: Either[OutputStream, URI] = Left(new FileOutputStream(new File(location)))

  override protected[task] def setup(): Unit = {
    assert(location.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

}

object ExportToFile extends TaskLike {

  override val taskName = database.ExportToFile.taskName

  override val defaultConfig = ConfigFactory.empty().withValue("export",TeraExportSetting.defaultConfig.root())

  override def apply(name: String,config: Config) = {
    val exportSettings = TeraExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    new ExportToFile(name, sql, FileSystemUtil.makeURI(config.as[String]("location")), connectionProfile, exportSettings)
  }

  override val info: String = database.ExportToFile.info

  override val desc: String = database.ExportToFile.desc

  override val paramConfigDoc = database.ExportToFile.paramConfigDoc(1025)

  override val fieldDefinition = database.ExportToFile.fieldDefinition

}


package tech.artemisia.task.database.teradata

/**
  * Created by chlr on 6/26/16.
  */

import java.io.{File, FileOutputStream, OutputStream}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database
import tech.artemisia.task.database.{DBInterface, ExportTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.FileSystemUtil
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 6/10/16.
  */


class ExportToFile(override val name: String, override val sql: String, location: URI, override val connectionProfile: DBConnection
                   ,override val exportSetting: TeraExportSetting)
  extends database.ExportToFile(name: String, sql: String, location, connectionProfile: DBConnection ,exportSetting) {

  override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, mode = exportSetting.mode, exportSetting.session)

  override val target: Either[OutputStream, URI] = Left(new FileOutputStream(new File(location)))

  override protected[task] def setup(): Unit = {
    assert(location.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

  override val supportedModes: Seq[String] = ExportToFile.supportedModes
}

object ExportToFile extends ExportTaskHelper {

  override val defaultConfig = ConfigFactory.empty().withValue("export",TeraExportSetting.defaultConfig.root())

  override def apply(name: String,config: Config) = {
    val exportSettings = TeraExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val sql = config.asInlineOrFile("sql")
    new ExportToFile(name, sql, FileSystemUtil.makeURI(config.as[String]("location")), connectionProfile, exportSettings)
  }

  override def paramConfigDoc = super.paramConfigDoc withValue ("export",TeraExportSetting.structure.root())

  override def fieldDefinition = super.fieldDefinition + ("export" -> TeraExportSetting.fieldDescription)

  override def supportedModes: Seq[String] = "default" :: "fastexport" :: Nil

  override val defaultPort: Int = 1025

}


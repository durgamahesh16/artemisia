package tech.artemisia.task.database.mysql

import java.io.{File, FileOutputStream}
import java.net.URI

import com.typesafe.config.Config
import tech.artemisia.task.database
import tech.artemisia.task.database.{BasicExportSetting, DBInterface, ExportTaskHelper}
import tech.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 4/22/16.
 */

class ExportToFile(name: String, sql: String, location: URI ,connectionProfile: DBConnection ,exportSettings: BasicExportSetting)
  extends database.ExportToFile(name, sql, location, connectionProfile, exportSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, mode=exportSettings.mode)

  override val supportedModes: Seq[String] = ExportToFile.supportedModes

  override protected[task] def setup(): Unit = {
    require(location.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

  override val target = exportSettings.mode match {
    case "default" => Left(new FileOutputStream(new File(location)))
    case "bulk" => Right(location)
  }

}

object ExportToFile extends ExportTaskHelper {

  override def apply(name: String,config: Config) = ExportTaskHelper.create[ExportToFile](name, config)

  override val defaultPort: Int = 3306

  override def supportedModes: Seq[String] = "default" :: "bulk" :: Nil

}



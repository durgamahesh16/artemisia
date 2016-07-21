package tech.artemisia.task.database.mysql

import java.io.{File, FileOutputStream}
import java.net.URI

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{BasicExportSetting, DBConnection}
import tech.artemisia.task.{TaskLike, database}

/**
 * Created by chlr on 4/22/16.
 */

class ExportToFile(name: String, sql: String, location: URI ,connectionProfile: DBConnection ,exportSettings: BasicExportSetting)
  extends database.ExportToFile(name, sql, location, connectionProfile, exportSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, mode=exportSettings.mode)

  override protected[task] def setup(): Unit = {
    assert(location.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

  override val target = exportSettings.mode match {
    case "default" => Left(new FileOutputStream(new File(location)))
    case "bulk" => Right(location)
  }

}

object ExportToFile extends TaskLike {

  override val taskName = database.ExportToFile.taskName

  override val defaultConfig: Config = database.ExportToFile.defaultConfig

  override def apply(name: String,config: Config) = database.ExportToFile.create[ExportToFile](name, config)

  override val info: String = database.ExportToFile.info

  override val desc: String = database.ExportToFile.desc

  override val paramConfigDoc = database.ExportToFile.paramConfigDoc(3306)

  override val fieldDefinition = database.ExportToFile.fieldDefinition

}



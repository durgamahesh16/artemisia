package tech.artemisia.task.database.mysql

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.TaskLike
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{BasicExportSetting, DBConnection}
import tech.artemisia.task.database

/**
 * Created by chlr on 4/22/16.
 */

class ExportToFile(name: String, sql: String, connectionProfile: DBConnection ,exportSettings: BasicExportSetting)
  extends database.ExportToFile(name: String, sql: String, connectionProfile: DBConnection ,exportSettings: BasicExportSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, mode=exportSettings.mode)

  override protected[task] def setup(): Unit = {
    assert(exportSettings.file.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

}

object ExportToFile extends TaskLike {

  override val taskName = database.ExportToFile.taskName

  override val defaultConfig: Config = ConfigFactory.empty()
              .withValue("export",BasicExportSetting.defaultConfig.root())

  override def apply(name: String,config: Config) = database.ExportToFile.create[ExportToFile](name, config)

  override val info: String = database.ExportToFile.info

  override val desc: String = database.ExportToFile.desc

  override val paramConfigDoc = database.ExportToFile.paramConfigDoc(3306)

  override val fieldDefinition = database.ExportToFile.fieldDefinition

}



package tech.artemisia.task.database.teradata

/**
  * Created by chlr on 6/26/16.
  */


import com.typesafe.config.Config
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{DBConnection, ExportSetting}

/**
  * Created by chlr on 6/10/16.
  */


class ExportToFile(name: String, sql: String, connectionProfile: DBConnection ,exportSettings: ExportSetting)
  extends database.ExportToFile(name: String, sql: String, connectionProfile: DBConnection ,exportSettings: ExportSetting) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, mode = exportSettings.mode)

  override protected[task] def setup(): Unit = {
    assert(exportSettings.file.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

}

object ExportToFile extends TaskLike {

  override val taskName = database.ExportToFile.taskName

  override def apply(name: String,config: Config) = database.ExportToFile.create[ExportToFile](name, config)

  override val info: String = database.ExportToFile.info

  override val desc: String = database.ExportToFile.desc

  override def configStructure(component: String): String = database.ExportToFile.configStructure(component, 5432)

  override val fieldDefinition = database.ExportToFile.fieldDefinition
}


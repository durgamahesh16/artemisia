package tech.artemisia.task.database.teradata

/**
  * Created by chlr on 6/26/16.
  */

import tech.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}

/**
  * Created by chlr on 6/10/16.
  */


class ExportToFile(override val name: String, override val sql: String, override val connectionProfile: DBConnection
                   ,override val exportSettings: TeraExportSetting)
  extends database.ExportToFile(name: String, sql: String, connectionProfile: DBConnection ,exportSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, mode = exportSettings.mode,
    exportSettings.session)

  override protected[task] def setup(): Unit = {
    assert(exportSettings.file.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

}

object ExportToFile extends TaskLike {

  override val taskName = database.ExportToFile.taskName

  override def apply(name: String,config: Config) = {
    ExportToFile(database.ExportToFile.create[ExportToFile](name, config), config.as[Int]("export.session"))
  }

  def apply(task: database.ExportToFile, session: Int): ExportToFile = {
      new ExportToFile(task.name, task.sql, task.connectionProfile, TeraExportSetting(task.exportSettings, session))
  }

  override val info: String = database.ExportToFile.info

  override val desc: String = database.ExportToFile.desc

  override def configStructure(component: String): String = database.ExportToFile.configStructure(component, 1025)

  override val fieldDefinition = database.ExportToFile.fieldDefinition

}


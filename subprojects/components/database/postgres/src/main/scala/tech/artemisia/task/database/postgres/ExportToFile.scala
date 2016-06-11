package tech.artemisia.task.database.postgres

import com.typesafe.config.Config
import tech.artemisia.task.TaskLike
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{ConnectionProfile, ExportSetting}

/**
  * Created by chlr on 6/10/16.
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

  override def apply(name: String,config: Config) = tech.artemisia.task.database.ExportToFile.create[ExportToFile](name, config)

  override val info: String = tech.artemisia.task.database.ExportToFile.info

  override def doc(component: String): String = tech.artemisia.task.database.ExportToFile.doc(component)

}


package tech.artemisia.task.database.teradata

/**
  * Created by chlr on 6/26/16.
  */

import java.io.{File, FileOutputStream, OutputStream}
import java.net.URI

import com.typesafe.config.Config
import tech.artemisia.task.database
import tech.artemisia.task.database.{DBInterface, ExportTaskHelper}
import tech.artemisia.task.settings.{DBConnection, ExportSetting}

/**
  * Created by chlr on 6/10/16.
  */


class ExportToFile(override val name: String, override val sql: String, location: URI, override val connectionProfile: DBConnection
                   ,override val exportSetting: ExportSetting)
  extends database.ExportToFile(name: String, sql: String, location, connectionProfile: DBConnection ,exportSetting) {

  override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, mode = exportSetting.mode)

  override val target: Either[OutputStream, URI] = Left(new FileOutputStream(new File(location)))

  override protected[task] def setup(): Unit = {
    assert(location.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

  override val supportedModes: Seq[String] = ExportToFile.supportedModes
}


object ExportToFile extends ExportTaskHelper {

  override def apply(name: String,config: Config) = ExportTaskHelper.create[ExportToFile](name, config)

  override val defaultPort: Int = 1025

  override def supportedModes: Seq[String] = "default" :: "fastexport" :: Nil

}




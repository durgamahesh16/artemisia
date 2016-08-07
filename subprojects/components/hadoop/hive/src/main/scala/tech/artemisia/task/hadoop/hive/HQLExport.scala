package tech.artemisia.task.hadoop.hive

import java.io.{File, FileOutputStream, OutputStream}
import java.net.URI
import com.typesafe.config.Config
import tech.artemisia.task.database.{BasicExportSetting, DBInterface, ExportTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, database}

/**
  * Created by chlr on 8/2/16.
  */
class HQLExport(taskName: String, sql: String, location: URI, connectionProfile: DBConnection, exportSetting: BasicExportSetting)
    extends database.ExportToFile(taskName, sql, location, connectionProfile, exportSetting) {

  override val dbInterface: DBInterface = new HiveServerDBInterface(connectionProfile)

  override val supportedModes: Seq[String] = "default" :: Nil

  override val target: Either[OutputStream, URI] = Left(new FileOutputStream(new File(location)))

}

object HQLExport extends ExportTaskHelper {

  override val taskName = "HQLExport"

  override def supportedModes: Seq[String] = "default" :: Nil

  override val defaultPort: Int = 10000

  override def apply(name: String, config: Config): Task = {
    database.ExportTaskHelper.create[HQLExport](name, config)
  }

}


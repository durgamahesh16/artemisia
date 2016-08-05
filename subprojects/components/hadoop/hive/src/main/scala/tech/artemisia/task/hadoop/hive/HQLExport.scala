package tech.artemisia.task.hadoop.hive

import java.io.{File, FileOutputStream, OutputStream}
import java.net.URI
import tech.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config.{Config, ConfigValue}
import tech.artemisia.task.database.{BasicExportSetting, DBInterface, ExportTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, database}

/**
  * Created by chlr on 8/2/16.
  */
class HQLExport(taskName: String, sql: String, location: URI, connectionProfile: Option[DBConnection], exportSetting: BasicExportSetting)
    extends database.ExportToFile(taskName, sql, location, connectionProfile.getOrElse(DBConnection.getDummyConnection), exportSetting) {

  override val dbInterface: DBInterface = DBInterfaceFactory.getDBInterface(connectionProfile)

  override val supportedModes: Seq[String] = "default" :: Nil

  override val target: Either[OutputStream, URI] = Left(new FileOutputStream(new File(location)))

}

object HQLExport extends ExportTaskHelper {

  override val taskName = "HQLExport"

  override def supportedModes: Seq[String] = "default" :: Nil

  override val defaultPort: Int = 10000

  override def apply(name: String, config: Config): Task = {
    val sql = config.asInlineOrFile("sql")
    val location = new URI(config.as[String]("file"))
    val connectionProfile = config.getAs[ConfigValue]("dsn") map DBConnection.parseConnectionProfile
    val exportSetting = BasicExportSetting(config.as[Config]("export"))
    new HQLExport(name, sql, location, connectionProfile, exportSetting)
  }

}


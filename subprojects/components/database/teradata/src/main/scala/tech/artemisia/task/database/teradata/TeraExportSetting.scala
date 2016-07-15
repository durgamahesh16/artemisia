package tech.artemisia.task.database.teradata

import java.net.URI
import tech.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config.{Config, ConfigValueFactory}
import tech.artemisia.task.settings
import tech.artemisia.task.settings.BasicExportSetting

/**
  * Created by chlr on 6/30/16.
  */
case class TeraExportSetting(override val file: URI, override val header: Boolean = false, override val delimiter: Char = ',',
                             override val quoting: Boolean = false, override val quotechar: Char = '"',
                             override val escapechar: Char = '\\', override val mode: String = "default", session: Int = 1)
  extends settings.ExportSetting(file, header, delimiter, quoting, quotechar, escapechar, mode)

object TeraExportSetting {

  val structure = BasicExportSetting.structure.withValue("session", ConfigValueFactory.fromAnyRef("1"))

  val fieldDescription = BasicExportSetting.fieldDescription + ("session" -> "number of sessions to use.")

  val defaultConfig = BasicExportSetting.defaultConfig.withValue("session" , ConfigValueFactory.fromAnyRef(1))


  def apply(config: Config): TeraExportSetting = {
    val loadSetting = BasicExportSetting(config)
    TeraExportSetting(loadSetting.file, loadSetting.header, loadSetting.delimiter, loadSetting.quoting
      ,loadSetting.quotechar, loadSetting.escapechar, loadSetting.mode,config.as[Int]("session"))
  }

}

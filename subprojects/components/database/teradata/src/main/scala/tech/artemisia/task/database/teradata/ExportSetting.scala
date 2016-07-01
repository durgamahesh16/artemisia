package tech.artemisia.task.database.teradata

import java.net.URI
import tech.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config.Config
import tech.artemisia.task.settings

/**
  * Created by chlr on 6/30/16.
  */
case class ExportSetting(override val file: URI, override val header: Boolean = false, override val delimiter: Char = ',',
                         override val quoting: Boolean = false, override val quotechar: Char = '"',
                         override val escapechar: Char = '\\', mode: String = "default", session: Int = 1)
  extends settings.ExportSetting(file, header, delimiter, quoting, quotechar, escapechar, mode) {

}

object ExportSetting {

  def apply(config: Config) = {
    val setting = settings.ExportSetting(config)
    ExportSetting(setting.file, setting.header, setting.delimiter, setting.quoting, setting.quotechar, setting.escapechar
      , setting.mode, config.as[Int]("session"))
  }

}

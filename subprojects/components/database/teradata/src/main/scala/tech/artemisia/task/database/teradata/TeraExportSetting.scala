package tech.artemisia.task.database.teradata

import java.net.URI

import com.typesafe.config.ConfigValueFactory
import tech.artemisia.task.settings
import tech.artemisia.task.settings.{BasicExportSetting, ExportSetting}

/**
  * Created by chlr on 6/30/16.
  */
case class TeraExportSetting(override val file: URI, override val header: Boolean = false, override val delimiter: Char = ',',
                             override val quoting: Boolean = false, override val quotechar: Char = '"',
                             override val escapechar: Char = '\\', override val mode: String = "default", session: Int = 1)
  extends settings.ExportSetting(file, header, delimiter, quoting, quotechar, escapechar, mode)

object TeraExportSetting {

  val structure =
    s"""|{
        |  file = /var/tmp/file.out @required
        |  header =  yes @default(false) @type(boolean)
        |  delimiter = '|' @default(",") @type(char)
        |  quoting = yes @default(false) @type(boolean)
        |  quotechar = "\"" @default('"') @type(char)
        |  escapechar = "\\" @default("\") @type(char)
        |  sql = "select * from table" @required
        |  mode = @default("default")
        |  session = 1
        |}""".stripMargin

  val fieldDescription = BasicExportSetting.fieldDescription :+ ("session" -> "")

  val defaultConfig = BasicExportSetting.defaultConfig.withValue("session" , ConfigValueFactory.fromAnyRef(1))

  def apply(setting: ExportSetting, session: Int): TeraExportSetting = {
    new TeraExportSetting(setting.file, setting.header, setting.delimiter, setting.quoting, setting.quotechar, setting.escapechar, setting.mode, session)
  }

}

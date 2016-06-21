package tech.artemisia.task.settings

import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.util.FileSystemUtil
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/13/16.
 */

/**
 * A case class for storing Export settings
 * @param file Target file to store exported data
 * @param header include header in file
 * @param delimiter delimiter of the file
 * @param quoting enable/disable quoting fields
 * @param quotechar character to be used for quoting if enabled
 * @param escapechar escape character to be used for escaping special characters
 */
  case class ExportSetting(file: URI, header: Boolean = false, override val delimiter: Char = ',',
                         override val quoting: Boolean = false, override val quotechar: Char = '"',
                         override val escapechar: Char = '\\', mode: String = "default")
  extends CSVSettings(delimiter, quoting, quotechar, escapechar)

object ExportSetting {

  val structure =
    s"""|{
        |  header =  yes @default(false) @type(boolean)
        |  delimiter = '|' @default(",") @type(char)
        |  quoting = yes @default(false) @type(boolean)
        |  quotechar = "\"" @default('"') @type(char)
        |  escapechar = "\\" @default("\") @type(char)
        |  mode = @default("default")
        |}""".stripMargin

  val fieldDescription = Seq[(String, String)](
    "file" -> "location of the file to which data is to be exported. eg: /var/tmp/output.txt",
    "header" -> "boolean literal to enable/disable header",
    "delimiter" -> "character to be used for delimiter",
    "quoting" -> "boolean literal to enable/disable quoting of fields.",
    "quotechar" -> "quotechar to use if quoting is enabled.",
    "escapechar" -> "escape character use for instance to escape delimiter values in field",
    "sql" -> "SQL query whose result-set will be exported.",
    "sqlfile" -> "used in place of sql key to pass the file containing the SQL"
  )


  val default_config = ConfigFactory parseString
    """
      | {
      |	  header = false
      |	  delimiter = ","
      |	  quoting = no,
      |	  quotechar = "\""
      |   escapechar = "\\"
      |   mode = "default"
      |	}
    """.stripMargin

  def apply(inputConfig: Config): ExportSetting = {
    val config = inputConfig withFallback default_config
    ExportSetting(
      file = FileSystemUtil.makeURI(config.as[String]("file")),
      header = config.as[Boolean]("header"),
      delimiter = config.as[Char]("delimiter"),
      quoting = config.as[Boolean]("quoting"),
      escapechar = config.as[Char]("escapechar"),
      quotechar =  config.as[Char]("quotechar"),
      mode = config.as[String]("mode")
    )
  }
}

package tech.artemisia.task.settings

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.ConfigurationNode
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 4/13/16.
  */

/**
  * A case class for storing Export settings
  *
  * @param header     include header in file
  * @param delimiter  delimiter of the file
  * @param quoting    enable/disable quoting fields
  * @param quotechar  character to be used for quoting if enabled
  * @param escapechar escape character to be used for escaping special characters
  */
case class BasicExportSetting(override val header: Boolean = false, override val delimiter: Char = ',',
                              override val quoting: Boolean = false, override val quotechar: Char = '"',
                              override val escapechar: Char = '\\', override val mode: String = "default")
  extends ExportSetting(header, delimiter, quoting, quotechar, escapechar, mode)

object BasicExportSetting extends ConfigurationNode[BasicExportSetting] {

  override val structure = ConfigFactory parseString
    raw"""|{
          |  header =  "yes @default(false) @type(boolean)"
          |  delimiter = "| @default(,) @type(char)"
          |  quoting = "yes @default(false) @type(boolean)"
          |  quotechar = "'\"' @default(\") @type(char)"
          |  escapechar = "'\\' @default(\\) @type(char)"
          |  sql = "select * from table @required"
          |  mode = "default @default(default)"
          |}""".stripMargin

  override val fieldDescription = Map[String, Any](
    "header" -> "boolean literal to enable/disable header",
    "delimiter" -> "character to be used for delimiter",
    "quoting" -> "boolean literal to enable/disable quoting of fields.",
    "quotechar" -> "quotechar to use if quoting is enabled.",
    "escapechar" -> "escape character use for instance to escape delimiter values in field",
    "mode" -> ("modes of export. supported modes are" -> Seq("default", "bulk")),
    "sql" -> "SQL query whose result-set will be exported.",
    "sqlfile" -> "used in place of sql key to pass the file containing the SQL"
  )


  override val defaultConfig = ConfigFactory parseString
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

  def apply(config: Config): BasicExportSetting = {
    BasicExportSetting(
      header = config.as[Boolean]("header"),
      delimiter = config.as[Char]("delimiter"),
      quoting = config.as[Boolean]("quoting"),
      escapechar = config.as[Char]("escapechar"),
      quotechar = config.as[Char]("quotechar"),
      mode = config.as[String]("mode")
    )
  }
}

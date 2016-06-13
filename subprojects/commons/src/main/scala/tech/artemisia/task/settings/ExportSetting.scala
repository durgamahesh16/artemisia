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

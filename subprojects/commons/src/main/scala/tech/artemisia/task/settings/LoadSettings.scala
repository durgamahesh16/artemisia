package tech.artemisia.task.settings

import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.URIParser

/**
 * Created by chlr on 4/30/16.
 */

/**
 * Load settings definition
 */
case class LoadSettings(location: URI, skipRows: Int = 0, override val delimiter: Char = ',', override val quoting: Boolean = false,
                        override val quotechar: Char = '"', override val escapechar: Char = '\\', mode: String = "default",
                        rejectFile: Option[String] = None, errorTolerance: Option[Double] = None) extends
    CSVSettings(delimiter, quoting, quotechar, escapechar) {

}

object LoadSettings {

  val structure =
 s"""|{
     | load-path = /var/tmp/file.txt @required
     | header = no @default(false) @type(boolean)
     | skip-lines = 0 @default(0) @type(int)
     | delimiter = '|' @default(',') @type(char)
     | quoting = no @default(false) @type(boolean)
     | quotechar = "\"" @default('"') @type(char)
     | escapechar = "\\" @default('\') @type(char)
     | mode = default @default("default") @type(string)
     | error-tolerence = 0.57 @default(2) @type(double,0,1)
     | error-file = /var/tmp/error_file.txt @required
     |}""".stripMargin

  val fieldDescription =
    """| load-path = path to load from (eg: /var/tmp/input.txt)
       | header = boolean field to enable/disable headers
       | skip-lines = number of lines to skip in he table
       | delimiter = delimiter of the file
       | quoting = boolean field to indicate if the file is quoted.
       | quotechar = character to be used for quoting
       | escapechar = escape character used in the file
       | mode = mode of loading the table
       | error-file = location of the file where rejected error records are saved.
       | error-tolerance = % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    """.stripMargin

  val default_config = ConfigFactory parseString
    """
      |{
      |	  header =  no
      |	  skip-lines = 0
      |	  delimiter = ","
      |	  quoting = no
      |	  quotechar = "\""
      |   escapechar = "\\"
      |   mode = default
      |   error-tolerence = 2
      |}
    """.stripMargin

  def apply(inputConfig: Config): LoadSettings = {
    val config = inputConfig withFallback default_config
    LoadSettings (
    location = URIParser.parse(config.as[String]("load-path")),
    skipRows = if (config.as[Int]("skip-lines") == 0) if (config.as[Boolean]("header")) 1 else 0 else config.as[Int]("skip-lines"),
    delimiter = config.as[Char]("delimiter"),
    quoting = config.as[Boolean]("quoting"),
    quotechar = config.as[Char]("quotechar"),
    escapechar = config.as[Char]("escapechar"),
    mode = config.as[String]("mode"),
    rejectFile = if (config.hasPath("error-file")) Some(config.as[String]("error-file")) else None,
    errorTolerance = config.getAs[Double]("error-tolerence")
    )
  }

}

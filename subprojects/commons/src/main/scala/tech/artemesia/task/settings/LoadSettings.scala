package tech.artemesia.task.settings

import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemesia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/30/16.
 */

/**
 * Load settings definition
 */
case class LoadSettings(location: URI, skipRows: Int = 0, override val delimiter: Char = ',', override val quoting: Boolean = false,
                        override val quotechar: Char = '"', override val escapechar: Char = '\\', mode: String = "default", rejectFile: Option[String] = None) extends
    CSVSettings(delimiter, quoting, quotechar, escapechar) {

}

object LoadSettings {

  val default_config = ConfigFactory parseString
    """
      |{
      |	header =  no
      |	skip-lines = 0
      |	delimiter = ","
      |	quoting = no
      |	quotechar = "\""
      | escapechar = "\\"
      | mode = generic
      | error-file = null
      |}
    """.stripMargin

  def apply(inputConfig: Config): LoadSettings = {
    val config = inputConfig withFallback default_config
    LoadSettings (
    location = new URI(config.as[String]("load-path")),
    skipRows = if (config.as[Int]("skip-lines") == 0) if (config.as[Boolean]("header")) 1 else 0 else config.as[Int]("skip-lines"),
    delimiter = config.as[Char]("delimiter"),
    quoting = config.as[Boolean]("quoting"),
    quotechar = config.as[Char]("quotechar"),
    escapechar = config.as[Char]("escapechar"),
    mode = config.as[String]("mode"),
    rejectFile = if (config.getIsNull("error-file")) None else Some(config.as[String]("error-file"))
    )
  }

}
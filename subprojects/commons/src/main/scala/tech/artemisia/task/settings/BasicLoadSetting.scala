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
case class BasicLoadSetting(override val location: URI, override val skipRows: Int = 0, override val delimiter: Char = ',',
                            override val quoting: Boolean = false, override val quotechar: Char = '"', override val escapechar: Char = '\\',
                            override val truncate: Boolean = false, override val mode: String = "default", override val batchSize: Int = 100,
                            override val errorTolerance: Option[Double] = None)
 extends LoadSetting(location, skipRows, delimiter, quoting, quotechar, escapechar,truncate ,mode, batchSize, errorTolerance)

object BasicLoadSetting {

  val structure = ConfigFactory parseString
 raw"""|{
     | load-path = "/var/tmp/file.txt @required"
     | header = "no @default(false) @type(boolean)"
     | skip-lines = "0 @default(0) @type(int)"
     | delimiter = "'|' @default(',') @type(char)"
     | quoting = "no @default(false) @type(boolean)"
     | quotechar = "\" @default('\"') @type(char)"
     | escapechar = "\" @default(\\) @type(char)"
     | truncate = "yes @type(boolean)"
     | mode = "default @default(default) @type(string)"
     | batch-size = "200 @default(100)"
     | error-tolerence = "0.57 @default(2) @type(double,0,1)"
     |}""".stripMargin

  val fieldDescription = Map(
     "load-path" -> "path to load from (eg: /var/tmp/input.txt)",
     "header" -> "boolean field to enable/disable headers",
     "skip-lines" -> "number of lines to skip in he table",
     "delimiter" -> "delimiter of the file",
     "quoting" -> "boolean field to indicate if the file is quoted.",
     "quotechar" -> "character to be used for quoting",
     "escapechar" -> "escape character used in the file",
     "truncate" -> "truncate the target table before loading data",
     "mode" -> "mode of loading the table",
     "batch-size" -> "loads into table will be grouped into batches of this size.",
     "error-tolerance" -> "% of data that is allowable to get rejected value ranges from (0.00 to 1.00)"
  )


  val defaultConfig = ConfigFactory parseString
    """
      |{
      |	  header =  no
      |	  skip-lines = 0
      |	  delimiter = ","
      |	  quoting = no
      |	  quotechar = "\""
      |   escapechar = "\\"
      |   truncate = false
      |   batch-size = 100
      |   mode = default
      |}
    """.stripMargin

  def apply(inputConfig: Config): BasicLoadSetting = {
    val config = inputConfig withFallback defaultConfig
    BasicLoadSetting (
    location = URIParser.parse(config.as[String]("load-path")),
    skipRows = if (config.as[Int]("skip-lines") == 0) if (config.as[Boolean]("header")) 1 else 0 else config.as[Int]("skip-lines"),
    delimiter = config.as[Char]("delimiter"),
    quoting = config.as[Boolean]("quoting"),
    quotechar = config.as[Char]("quotechar"),
    escapechar = config.as[Char]("escapechar"),
    mode = config.as[String]("mode"),
    truncate = config.as[Boolean]("truncate"),
    errorTolerance = config.getAs[Double]("error-tolerence"),
    batchSize = config.as[Int]("batch-size")
    )
  }

}

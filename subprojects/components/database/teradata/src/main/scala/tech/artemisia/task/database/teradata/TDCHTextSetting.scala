package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.ConfigurationNode
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/28/16.
  */

/**
  *
  * @param delimiter delimiter for fields
  * @param quoting boolean field to enabled/disable quoting
  * @param quoteChar quoting character that encloses the fields
  * @param nullString string to represent null values
  * @param escapedBy escape characters to be used.
  */
case class TDCHTextSetting(delimiter: Char = ',', quoting: Boolean = false, quoteChar: Char= '"', nullString: Option[String] = None,
                           escapedBy: Char = '\\') {


  val commandArgs = "-separator" :: "\\u" + Integer.toHexString('÷' | 0x10000).substring(1) :: Nil ++
    (if(quoting) "-enclosedby" :: quoteChar :: "-escapedby" :: escapedBy :: Nil else Nil) ++
    (if(nullString.isDefined) "-nullstring" :: nullString.get :: Nil else Nil)


}

object TDCHTextSetting extends ConfigurationNode[TDCHTextSetting] {

  override val defaultConfig: Config = ConfigFactory parseString
   """
     |{
     |  delimiter = ","
     |  quoting = no
     |  quote-char = "\""
     |  escape-char = "\\"
     |}
   """.stripMargin

  override def apply(config: Config): TDCHTextSetting = {
    TDCHTextSetting(
      config.as[Char]("delimiter"),
      config.as[Boolean]("quoting"),
      config.as[Char]("quote-char"),
      config.getAs[String]("null-string"),
      config.as[Char]("escape-char")
    )
  }

  override val structure: Config = ConfigFactory parseString
     """
       |{
       |  delimiter = "| @default(,)"
       |  quoting = "no @type(boolean)"
       |  quote-char = "\""
       |  escape-char = "\\"
       |}
       |
     """.stripMargin

  override val fieldDescription: Map[String, Any] = Map(
    "delimiter" -> "delimiter of the textfile",
    "quoting" -> "enable or disable quoting. both quote-char and escape-char fields are considered only when quoting is enabled",
    "quote-char" -> "character used for quoting",
    "escape-char" -> "escape character to be used. forward slash by default",
    "null-string" -> "string to represent null values"
  )
}

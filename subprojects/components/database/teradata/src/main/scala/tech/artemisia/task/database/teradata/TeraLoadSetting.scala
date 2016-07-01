package tech.artemisia.task.database.teradata

import java.net.URI

import com.typesafe.config.{Config, ConfigValueFactory}
import tech.artemisia.task.settings
import tech.artemisia.task.settings.BasicLoadSetting
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 6/30/16.
  */
case class TeraLoadSetting(override val location: URI, override val skipRows: Int = 0, override val delimiter: Char = ',',
                        override val quoting: Boolean = false, override val quotechar: Char = '"', override val escapechar: Char = '\\',
                        override val mode: String = "default", override val batchSize: Int = 100, override val rejectFile: Option[String] = None,
                        override val errorTolerance: Option[Double] = None, sessions: Int = 1)
  extends settings.LoadSetting(location, skipRows, delimiter, quoting, quotechar, escapechar, mode, batchSize, rejectFile, errorTolerance) {

}

object TeraLoadSetting {

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
        | batch-size = 200 @default(100)
        | error-tolerence = 0.57 @default(2) @type(double,0,1)
        | error-file = /var/tmp/error_file.txt @required
        | session = 1
        |}""".stripMargin

  val fieldDescription = BasicLoadSetting.fieldDescription.+:("session" -> "no of sessions used for the load.")

  val defaultConfig = BasicLoadSetting.defaultConfig.withValue("session",ConfigValueFactory.fromAnyRef(1))

  def apply(inputConfig: Config): TeraLoadSetting = {
    val config = inputConfig withFallback defaultConfig
    val loadSetting = BasicLoadSetting(inputConfig)
    TeraLoadSetting(loadSetting.location, loadSetting.skipRows, loadSetting.delimiter, loadSetting.quoting
      ,loadSetting.quotechar, loadSetting.escapechar, loadSetting.mode, loadSetting.batchSize,loadSetting.rejectFile
      ,loadSetting.errorTolerance, config.as[Int]("session"))
  }

}

package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigValueFactory}
import tech.artemisia.task.database.BasicExportSetting
import tech.artemisia.task.{ConfigurationNode, settings}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 6/30/16.
  */
case class TeraExportSetting(override val header: Boolean = false, override val delimiter: Char = ',',
                             override val quoting: Boolean = false, override val quotechar: Char = '"',
                             override val escapechar: Char = '\\', override val mode: String = "default", session: Int = 1)
  extends settings.ExportSetting(header, delimiter, quoting, quotechar, escapechar, mode)

object TeraExportSetting extends ConfigurationNode[TeraExportSetting]{

  val structure = BasicExportSetting.structure.withValue("session", ConfigValueFactory.fromAnyRef("1"))

  val fieldDescription = BasicExportSetting.fieldDescription ++
    Seq("session" -> "number of sessions to use.",
        "mode" -> ("export mode to be used" -> Seq("default", "fastexport"))
    )

  val defaultConfig = BasicExportSetting.defaultConfig.withValue("session" , ConfigValueFactory.fromAnyRef(1))


  def apply(config: Config): TeraExportSetting = {
    val loadSetting = BasicExportSetting(config)
    TeraExportSetting(loadSetting.header, loadSetting.delimiter, loadSetting.quoting
      ,loadSetting.quotechar, loadSetting.escapechar, loadSetting.mode,config.as[Int]("session"))
  }

}

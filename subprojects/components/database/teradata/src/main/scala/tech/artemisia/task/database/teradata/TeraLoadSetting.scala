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
                        override val truncate: Boolean = false,override val mode: String = "default",
                        override val batchSize: Int = 100, override val errorTolerance: Option[Double] = None,
                        recreateTable: Boolean = false, sessions: Int = 1)
  extends settings.LoadSetting(location, skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance)

object TeraLoadSetting {

  val structure = BasicLoadSetting.structure
            .withValue("session", ConfigValueFactory.fromAnyRef(""""x1 @default(small-load -> 1, fastload -> 10)""""))
            .withValue("recreate-table", ConfigValueFactory.fromAnyRef("no @default(false)"))

  val fieldDescription = BasicLoadSetting.fieldDescription ++: Map(
    "recreate-table" -> "drop and recreate the target table. This may be required for Fastload for restartablity",
    "session" -> "no of sessions used for the load"
  )

  val defaultConfig = BasicLoadSetting.defaultConfig
            .withValue("session",ConfigValueFactory.fromAnyRef(1))
            .withValue("recreate-table", ConfigValueFactory.fromAnyRef(false))


  def apply(config: Config): TeraLoadSetting = {
    val loadSetting = BasicLoadSetting(config)
    TeraLoadSetting(loadSetting.location, loadSetting.skipRows, loadSetting.delimiter, loadSetting.quoting
      ,loadSetting.quotechar, loadSetting.escapechar, loadSetting.truncate, loadSetting.mode, loadSetting.batchSize
      ,loadSetting.errorTolerance, config.as[Boolean]("recreate-table") ,config.as[Int]("session")
    )
  }

}

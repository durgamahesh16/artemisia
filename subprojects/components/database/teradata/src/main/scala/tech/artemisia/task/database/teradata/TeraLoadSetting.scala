package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigValueFactory}
import tech.artemisia.task.database.BasicLoadSetting
import tech.artemisia.task.{ConfigurationNode, settings}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.MemorySize

/**
  * Created by chlr on 6/30/16.
  */
case class TeraLoadSetting(override val skipRows: Int = 0, override val delimiter: Char = ',',
                        override val quoting: Boolean = false, override val quotechar: Char = '"', override val escapechar: Char = '\\',
                        override val truncate: Boolean = false,override val mode: String = "default",
                        override val batchSize: Int = 100, override val errorTolerance: Option[Double] = None,
                        recreateTable: Boolean = false, sessions: Int = 1, bulkLoadThreshold: Long = 104857600)
  extends settings.LoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance)

object TeraLoadSetting extends ConfigurationNode[TeraLoadSetting] {

  val structure = BasicLoadSetting.structure
            .withValue("session", ConfigValueFactory.fromAnyRef(""""x1 @default(small-load -> 1, fastload -> 10)""""))
            .withValue("recreate-table", ConfigValueFactory.fromAnyRef("no @default(false)"))
            .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M @info()"))

  val fieldDescription = BasicLoadSetting.fieldDescription ++: Map(
    "mode" -> ("mode of loading the table. The allowed modes are" -> Seq("fastload", "small", "auto")),
    "recreate-table" -> "drop and recreate the target table. This may be required for Fastload for restartablity",
    "session" -> "no of sessions used for the load",
    "bulk-threshold" -> "size of the source file(s) above which fastload mode will be selected if auto mode is enabled"
  )

  val defaultConfig = BasicLoadSetting.defaultConfig
            .withValue("session",ConfigValueFactory.fromAnyRef(1))
            .withValue("recreate-table", ConfigValueFactory.fromAnyRef(false))
            .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M"))


  def apply(config: Config): TeraLoadSetting = {
    val loadSetting = BasicLoadSetting(config)
    TeraLoadSetting(loadSetting.skipRows, loadSetting.delimiter, loadSetting.quoting
      ,loadSetting.quotechar, loadSetting.escapechar, loadSetting.truncate, loadSetting.mode, loadSetting.batchSize
      ,loadSetting.errorTolerance, config.as[Boolean]("recreate-table") ,config.as[Int]("session")
      ,config.as[MemorySize]("bulk-threshold").toBytes
    )
  }

}

package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigValueFactory}
import tech.artemisia.task.ConfigurationNode
import tech.artemisia.task.database.BasicLoadSetting
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.MemorySize

/**
  * Created by chlr on 6/30/16.
  */
case class TeraLoadSetting(override val skipRows: Int = 0,
                           override val delimiter: Char = ',',
                           override val quoting: Boolean = false,
                           override val quotechar: Char = '"',
                           override val escapechar: Char = '\\',
                           override val truncate: Boolean = false,
                           override val mode: String = "default",
                           override val batchSize: Int = 100,
                           override val errorTolerance: Option[Double] = None,
                           override val bulkLoadThreshold: Long = 104857600)
  extends BaseTeraLoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance, bulkLoadThreshold) {

  override def setting: String = {
    BasicLoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance).setting +
    s"bulk-threshold: $bulkLoadThreshold"
  }

  /**
    *
    * @param batchSize batch size
    * @param mode      load mode
    * @return
    */
  override def create(batchSize: Int, mode: String): BaseTeraLoadSetting = {
    copy(batchSize = batchSize, mode = mode)
  }

}

object TeraLoadSetting extends ConfigurationNode[TeraLoadSetting] {

  val structure = BasicLoadSetting.structure
            .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M @info()"))
            .withoutPath("batch-size")

  val fieldDescription = (BasicLoadSetting.fieldDescription - "batch-size") ++: Map(
    "mode" -> ("mode of loading the table. The allowed modes are" -> Seq("fastload", "small", "auto")),
    "bulk-threshold" -> "size of the source file(s) above which fastload mode will be selected if auto mode is enabled"
  )

  val defaultConfig = BasicLoadSetting.defaultConfig
            .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M"))


  def apply(config: Config): TeraLoadSetting = {
    val loadSetting = BasicLoadSetting(config)
    TeraLoadSetting(loadSetting.skipRows, loadSetting.delimiter, loadSetting.quoting
      ,loadSetting.quotechar, loadSetting.escapechar, loadSetting.truncate, loadSetting.mode
      ,if (loadSetting.mode == "fastload")  80000  else 1000
      ,loadSetting.errorTolerance,config.as[MemorySize]("bulk-threshold").toBytes
    )
  }


}

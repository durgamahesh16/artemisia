package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory, ConfigValueType}
import tech.artemisia.inventory.exceptions.InvalidSettingException
import tech.artemisia.task.database.BasicLoadSetting
import tech.artemisia.task.database.teradata.BaseTeraLoadSetting
import tech.artemisia.task.{ConfigurationNode, TaskContext}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.MemorySize

import scala.collection.JavaConverters._

/**
  * Created by chlr on 9/6/16.
  */
case class TPTLoadSetting(override val skipRows: Int = 0,
                          override val delimiter: Char = ',',
                          override val quoting: Boolean = false,
                          override val quotechar: Char = '"',
                          override val escapechar: Char = '\\',
                          override val truncate: Boolean = false,
                          override val batchSize: Int = 100,
                          override val errorTolerance: Option[Double] = None,
                          override val mode: String = "default",
                          override val bulkLoadThreshold: Long = 104857600L,
                          nullString: Option[String] =  None,
                          errorLimit: Int = 2000,
                          errorFile: String = TaskContext.getTaskFile("error.txt").toString,
                          loadOperatorAttrs: Map[String,(String,String)] = Map(),
                          dataConnectorAttrs: Map[String,(String,String)] = Map()) extends
  BaseTeraLoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode ,batchSize, errorTolerance, bulkLoadThreshold) {

  require(TPTLoadSetting.supportedModes contains mode, s"$mode is not supported. supported modes are ${TPTLoadSetting.supportedModes.mkString(",")}")

  override def setting: String = ???

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

object TPTLoadSetting extends ConfigurationNode[TPTLoadSetting] {

  val supportedModes = Seq("stream", "fastload", "auto")

  override val structure = BasicLoadSetting.structure
    .withValue("error-limit", ConfigValueFactory.fromAnyRef("1000 @default(2000)"))
    .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M @info()"))
    .withValue("error-file", ConfigValueFactory.fromAnyRef("/var/path/error.txt @optional"))
    .withValue("null-string", ConfigValueFactory.fromAnyRef("\\N @optional @info(marker string for null)"))

  override val fieldDescription = BasicLoadSetting.fieldDescription ++
    Map(
      "null-string" -> "marker string for null. default value is blank string",
      "error-limit" -> "maximum number of records allowed in error table",
      "error-file" -> "location of the reject file",
      "load-attrs" -> "miscellaneous load operator attributes",
      "dtconn-attrs" -> "miscellaneous data-connector operator attributes",
      "bulk-threshold" -> "size of the source file(s) above which fastload mode will be selected if auto mode is enabled"
    ) -- Seq("batch-size")

  override val defaultConfig = BasicLoadSetting.defaultConfig
    .withValue("error-limit", ConfigValueFactory.fromAnyRef(2000))
    .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M"))



  override def apply(config: Config): TPTLoadSetting = {
    val loadSetting = BasicLoadSetting(config)
    TPTLoadSetting(
      skipRows = if (config.as[Int]("skip-lines") == 0) if (config.as[Boolean]("header")) 1 else 0 else config.as[Int]("skip-lines"),
      loadSetting.delimiter,
      loadSetting.quoting,
      loadSetting.quotechar,
      loadSetting.escapechar,
      loadSetting.truncate,
      loadSetting.batchSize,
      loadSetting.errorTolerance,
      loadSetting.mode,
      config.as[MemorySize]("bulk-threshold").toBytes,
      config.getAs[String]("null-string"),
      config.as[Int]("error-limit"),
      config.getAs[String]("error-file").getOrElse(TaskContext.getTaskFile("error.txt").toString),
      parseAttributeNodes(config.as[Config]("load-attrs")),
      parseAttributeNodes(config.as[Config]("dtconn-attrs"))
    )
  }


  def parseAttributeNodes(node: Config) = {
    val map = node.root.keySet().asScala map {
      x => node.as[ConfigValue](x).valueType() match {
        case ConfigValueType.STRING => x -> ("VARCHAR", node.as[String](x))
        case ConfigValueType.OBJECT =>
          val valueNode = node.as[Config](x)
          x -> (valueNode.as[String]("type") -> valueNode.as[String]("value"))
        case _ => throw new InvalidSettingException(s"operator attributes $x can only either be a string or config object")
      }
    }
    map.toMap
  }

}





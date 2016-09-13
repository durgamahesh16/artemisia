package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory, ConfigValueType}
import tech.artemisia.inventory.exceptions.InvalidSettingException
import tech.artemisia.task.database.BasicLoadSetting
import tech.artemisia.task.settings.LoadSetting
import tech.artemisia.task.{ConfigurationNode, TaskContext}
import tech.artemisia.util.HoconConfigUtil.Handler

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
                          errorLimit: Int = 2000,
                          errorFile: String = TaskContext.getTaskFile("error.txt").toString,
                          loadOperatorAttrs: Map[String,(String,String)] = Map(),
                          dataConnectorAttrs: Map[String,(String,String)] = Map()) extends
  LoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode ,batchSize, errorTolerance) {

  override def setting: String = ???
}

object TPTLoadSetting extends ConfigurationNode[TPTLoadSetting] {

  override val structure = BasicLoadSetting.structure
    .withValue("error-limit", ConfigValueFactory.fromAnyRef("1000 @default(2000)"))
    .withValue("error-file", ConfigValueFactory.fromAnyRef("/var/path/error.txt @optional"))

  override val fieldDescription = BasicLoadSetting.fieldDescription -- Seq("batch-size")

  override val defaultConfig = BasicLoadSetting.defaultConfig
    .withValue("error-limit", ConfigValueFactory.fromAnyRef(2000))


  override def apply(config: Config): TPTLoadSetting = {
    val loadSetting = BasicLoadSetting(config)
    TPTLoadSetting(
      loadSetting.skipRows,
      loadSetting.delimiter,
      loadSetting.quoting,
      loadSetting.quotechar,
      loadSetting.escapechar,
      loadSetting.truncate,
      loadSetting.batchSize,
      loadSetting.errorTolerance,
      loadSetting.mode,
      config.as[Int]("error-limit"),
      config.getAs[String]("error-file").getOrElse(TaskContext.getTaskFile("error.txt").toString),
      parseAttributeNodes(config.as[Config]("load-attrs")),
      parseAttributeNodes(config.as[Config]("dataconnector-attrs"))
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





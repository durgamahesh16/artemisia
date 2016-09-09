package tech.artemisia.task.database.teradata

import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory, ConfigValueType}
import tech.artemisia.task.ConfigurationNode
import tech.artemisia.task.database.BasicLoadSetting
import tech.artemisia.task.settings.LoadSetting
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
                          errorLimit: Int = 2000,
                          loadOperatorAttrs: Map[String,(String,String)] = Map(),
                          dataConnectorAttrs: Map[String,(String,String)] = Map()) extends
  LoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, "fastload" ,batchSize, errorTolerance) {

  override def setting: String = ???
}

object TPTLoadSetting extends ConfigurationNode[TPTLoadSetting] {

  override val structure = BasicLoadSetting.structure
    .withValue("error-limit", ConfigValueFactory.fromAnyRef("1000 @default(2000)"))
    .withoutPath("mode")

  override val fieldDescription = BasicLoadSetting.fieldDescription -- Seq("mode", "batch-size")

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
      config.as[Int]("error-limit"),
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
      }
    }
    map.toMap
  }

}





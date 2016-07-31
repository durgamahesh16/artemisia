package tech.artemisia.util

import java.io.File
import com.typesafe.config._
import org.apache.commons.lang3.StringEscapeUtils
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * Created by chlr on 3/18/16.
 */
object HoconConfigUtil {

  implicit val anyRefReader = new ConfigReader[AnyRef] {
    override def read(config: Config, path: String): AnyRef = {
      config.getAnyRef(path)
    }
  }

  implicit val anyRefListReader = new ConfigReader[List[AnyRef]] {
    override def read(config: Config, path: String): List[AnyRef] = {
      ( config.getAnyRefList(path).asScala map { _.asInstanceOf[AnyRef] } ).toList
    }
  }

  implicit val booleanReader = new ConfigReader[Boolean] {
    override def read(config: Config, path: String): Boolean = {
      config.getBoolean(path)
    }
  }

  implicit val booleanListReader = new ConfigReader[List[Boolean]] {
    override def read(config: Config, path: String): List[Boolean] = {
      config.getBooleanList(path).asScala.toList map { _.booleanValue() }
    }
  }

  implicit val byteReader = new ConfigReader[Byte] {
    override def read(config: Config, path: String): Byte = {
      config.getInt(path).asInstanceOf[Byte]
    }
  }

  implicit val configReader = new ConfigReader[Config] {
    override def read(config: Config, path: String): Config = {
      config.getConfig(path)
    }
  }

  implicit val configValueReader = new ConfigReader[ConfigValue] {
    override def read(config: Config, path: String): ConfigValue = {
      config.getValue(path)
    }
  }

  implicit val configListReader = new ConfigReader[List[Config]] {
    override def read(config: Config, path: String): List[Config] = {
      config.getConfigList(path).asScala.toList
    }
  }

  implicit val doubleReader = new ConfigReader[Double] {
    override def read(config: Config, path: String): Double = {
      config.getDouble(path)
    }
  }

  implicit val doubleListReader = new ConfigReader[List[Double]] {
    override def read(config: Config, path: String): List[Double] = {
      config.getDoubleList(path).asScala.toList map {_.toDouble }
    }
  }

  implicit val durationReader = new ConfigReader[FiniteDuration] {
    override def read(config: Config, path: String): FiniteDuration = {
      DurationParser(config.getString(path)).getFiniteDuration
    }
  }


  implicit val durationListReader = new ConfigReader[List[FiniteDuration]] {
    override def read(config: Config, path: String): List[FiniteDuration] = {
     config.getStringList(path).asScala.map(x => DurationParser(x).getFiniteDuration).toList
    }
  }

  implicit val intReader = new ConfigReader[Int] {
    override def read(config: Config, path: String): Int = {
      config.getInt(path)
    }
  }

  implicit val intListReader = new ConfigReader[List[Int]] {
    override def read(config: Config, path: String): List[Int] = {
      config.getIntList(path).asScala.toList map { _.toInt }
    }
  }

  implicit val longReader = new ConfigReader[Long] {
    override def read(config: Config, path: String): Long = {
      config.getLong(path)
    }
  }

  implicit val longListReader = new ConfigReader[List[Long]] {
    override def read(config: Config, path: String): List[Long] = {
      config.getLongList(path).asScala.toList map { _.toLong }
    }
  }

  implicit val memoryReader = new ConfigReader[MemorySize] {
    override def read(config: Config, path: String): MemorySize = {
      new MemorySize(config.getString(path))
    }
  }

  implicit val memoryListReader = new ConfigReader[List[MemorySize]] {
    override def read(config: Config, path: String): List[MemorySize] = {
      config.getStringList(path).asScala.map(new MemorySize(_)).toList
    }
  }


  implicit val charReader = new ConfigReader[Char] {
    override def read(config: Config, path: String): Char = {
      val data = config.getString(path)
      val parsedData = data.length match {
        case 1 => data
        case _ => StringEscapeUtils.unescapeJava(data)
      }
      require(parsedData.length == 1, "Character length is not 1")
      parsedData.toCharArray.apply(0)
    }
  }

  implicit val stringReader = new ConfigReader[String] {
    override def read(config: Config, path: String): String = {
      val str = config.getString(path)
      HoconConfigEnhancer.stripLeadingWhitespaces(str)
    }
  }

  implicit val stringListReader = new ConfigReader[List[String]] {
    override def read(config: Config, path: String): List[String] = {
      config.getStringList(path).asScala.toList
    }
  }

  /**
   *
   * @param config implicit function that converts Config to ConfigResolver object
   * @return ConfigResolver object
   */
  implicit def configToConfigEnhancer(config: Config): HoconConfigEnhancer = {
    new HoconConfigEnhancer(config)
  }

  def render(configValue: ConfigValue, indent: Int = 0): String = {
    configValue match {
      case config: ConfigObject => {
        val result =  s"""|{
                          |${config.keySet().asScala.toList.sorted map { x => s"   $x = ${render(config.toConfig.getValue(s""""$x""""), indent+1)}" } mkString s"${System.lineSeparator()}" }
                          |}""".stripMargin
        result.split(System.lineSeparator()) map { x => s"${" "*indent}$x" } mkString System.lineSeparator()
      }
      case config: ConfigList => {
        s"""|[${config.unwrapped().asScala map { ConfigValueFactory.fromAnyRef } map { render(_, indent+1) } mkString ", "}]""".stripMargin
      }
      case config => {
        config.render(ConfigRenderOptions.concise())
      }
    }
  }

  trait ConfigReader[T] {
    def read(config: Config, path: String): T
  }

  implicit class Handler(val config: Config) {

    def asMap[T: ConfigReader](key: String): Map[String,T] = {
      val configObject = config.getConfig(key).root()
      val result = configObject.keySet().asScala map { x => x -> configObject.toConfig.as[T](x) }
      result.toMap
    }

    def getAs[T: ConfigReader](key: String): Option[T] = {
      if (config.hasPath(key)) Some(as[T](key)) else None
    }

    def as[T: ConfigReader](key: String): T = {
      implicitly[ConfigReader[T]].read(config, key)
    }

    def asFile(key: String): String = {
      HoconConfigEnhancer.readFileContent(new File(config.getString(key)))
    }

  }

}



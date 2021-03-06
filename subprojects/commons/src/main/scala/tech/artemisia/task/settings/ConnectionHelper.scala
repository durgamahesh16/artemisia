package tech.artemisia.task.settings

import com.typesafe.config.{ConfigFactory, ConfigValueType, ConfigValue, Config}
import tech.artemisia.core.Keywords
import tech.artemisia.task.TaskContext
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 6/16/16.
 */


trait ConnectionHelper {

  type T

  def apply(config: Config): T

  def apply(connectionName: String): T = {
    this.apply(TaskContext.payload.as[Config](s"${Keywords.Config.CONNECTION_SECTION}.$connectionName"))
  }

  /**
   *
   * @param config input config that has a node dsn
   * @return
   */
  def parseConnectionProfile(config: ConfigValue) = {
    config.valueType() match {
      case ConfigValueType.STRING => this.apply(config.unwrapped().asInstanceOf[String])
      case ConfigValueType.OBJECT => this.apply(ConfigFactory.empty withFallback config)
      case x @ _ => throw new IllegalArgumentException(s"connection value must either be an object or string name}")
    }
  }

}
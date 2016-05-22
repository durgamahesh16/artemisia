package tech.artemisia.task.settings

import com.typesafe.config._
import tech.artemisia.util.HoconConfigUtil
import HoconConfigUtil.Handler
import tech.artemisia.core.Keywords
import tech.artemisia.core.Keywords.Connection
import tech.artemisia.task.TaskContext

/**
 * Created by chlr on 4/13/16.
 */

case class ConnectionProfile(hostname: String, username: String, password: String, default_database: String, port: Int)

object ConnectionProfile {

  def apply(config: Config): ConnectionProfile = {
       ConnectionProfile(
      hostname = config.as[String](Connection.HOSTNAME),
      username = config.as[String](Keywords.Connection.USERNAME),
      password = config.as[String](Keywords.Connection.PASSWORD),
      default_database = config.as[String](Keywords.Connection.DATABASE),
      port = config.as[Int](Keywords.Connection.PORT)
    )
  }

  def apply(connectionName: String): ConnectionProfile = {
    TaskContext.predefinedConnectionProfiles(connectionName)
  }

  /**
   *
   * @param config input config that has a node dsn
   * @return
   */
  def parseConnectionProfile(input: ConfigValue) = {
    input.valueType() match {
      case ConfigValueType.STRING => ConnectionProfile(input.unwrapped().asInstanceOf[String])
      case ConfigValueType.OBJECT => ConnectionProfile(ConfigFactory.empty withFallback input)
      case x @ _ => throw new IllegalArgumentException(s"Invalid connection node with type $x}")
    }
  }

}
package tech.artemisia.task.settings

import com.typesafe.config._
import tech.artemisia.core.Keywords
import tech.artemisia.core.Keywords.Connection
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/13/16.
 */

case class DBConnection(hostname: String, username: String, password: String, default_database: String, port: Int)

object DBConnection extends ConnectionHelper[DBConnection] {

  def structure(defaultPort: Int) =
 s""" |{
      |  ${Connection.HOSTNAME} = db-host @required
      |  ${Keywords.Connection.USERNAME} = username @required
      |  ${Keywords.Connection.PASSWORD} = password @required
      |  ${Keywords.Connection.DATABASE} = db @required
      |  ${Keywords.Connection.PORT} = db @default($defaultPort)
      | }
  """.stripMargin

  def apply(config: Config): DBConnection = {
       DBConnection(
      hostname = config.as[String](Connection.HOSTNAME),
      username = config.as[String](Keywords.Connection.USERNAME),
      password = config.as[String](Keywords.Connection.PASSWORD),
      default_database = config.as[String](Keywords.Connection.DATABASE),
      port = config.as[Int](Keywords.Connection.PORT)
    )
  }

}
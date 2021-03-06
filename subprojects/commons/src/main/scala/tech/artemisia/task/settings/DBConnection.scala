package tech.artemisia.task.settings

import com.typesafe.config._
import tech.artemisia.core.Keywords
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/13/16.
 */

sealed abstract class Connection

case class DBConnection(hostname: String, username: String, password: String, default_database: String, port: Int)
    extends Connection

object DBConnection extends ConnectionHelper {

  type T = DBConnection

  def structure(defaultPort: Int) =
  ConfigFactory parseString
 s""" |{
      |  ${Keywords.Connection.HOSTNAME} = "db-host @required"
      |  ${Keywords.Connection.USERNAME} = "username @required"
      |  ${Keywords.Connection.PASSWORD} = "password @required"
      |  ${Keywords.Connection.DATABASE} = "db @required"
      |  ${Keywords.Connection.PORT} = "$defaultPort @default($defaultPort)"
      | }
  """.stripMargin

  def apply(config: Config): DBConnection = {
       DBConnection(
      hostname = config.as[String](Keywords.Connection.HOSTNAME),
      username = config.as[String](Keywords.Connection.USERNAME),
      password = config.as[String](Keywords.Connection.PASSWORD),
      default_database = config.as[String](Keywords.Connection.DATABASE),
      port = config.as[Int](Keywords.Connection.PORT)
    )
  }

  /**
    * A dummy stand in DBConnection object that has all its properties/fields set to null.
    * @return
    */
  def getDummyConnection = {
    DBConnection(null, null, null, null, -1)
  }
}
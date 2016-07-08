package tech.artemisia.task.localhost

import com.typesafe.config.{ConfigFactory, Config}
import tech.artemisia.task.settings.ConnectionHelper
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 6/15/16.
 */

/**
 *
 * @param host smtp host address
 * @param port smtp port
 * @param username stmp username
 * @param password stmp password
 * @param ssl enable ssl
 * @param tls enable tls
 * @param from from address
 * @param replyTo default reply to address
 */
case class EmailConnection(host: String, port: Int, username: Option[String], password: Option[String],
                           ssl: Boolean, tls: Boolean, from: Option[String] = None, replyTo: Option[String] = None) {

}

object EmailConnection extends ConnectionHelper {

  type T = EmailConnection

  def structure =
    """|{
       |  host = "host @required"
       |  port = "-1 @required"
       |  username = "username"
       |  password = "password"
       |  ssl = "no @default(no) @type(boolean)"
       |  tls = "no @default(no) @type(boolean)"
       |  from = "xyz@example.com"
       |  reply-to ="xyx@example.com"
       |}""".stripMargin

  val fieldDefinition = Seq(
    "host" -> "SMTP host address",
    "port" -> "port of the stmp server",
    "username" -> "username used for authentication",
    "password" -> "password used for authentication",
    "ssl" -> "boolean field enabling ssl",
    "tls" -> "boolean field for enabling tls",
    "from" -> "from address to be used",
    "reply-to" -> "replies to the sent email will be addressed to this address"
  )

  private val defaultConfig = ConfigFactory parseString
   s"""
      |{
      |  ssl = no
      |  tls = no
      |}
    """.stripMargin

  def apply(inputConfig: Config): EmailConnection = {
    val config = inputConfig withFallback defaultConfig
    EmailConnection(
      host = config.as[String]("host"),
      port = config.as[Int]("port"),
      username = config.getAs[String]("username"),
      password = config.getAs[String]("password"),
      ssl = config.as[Boolean]("ssl"),
      tls = config.as[Boolean]("tls"),
      from = config.getAs[String]("from"),
      replyTo = config.getAs[String]("reply-to")
    )
  }


}

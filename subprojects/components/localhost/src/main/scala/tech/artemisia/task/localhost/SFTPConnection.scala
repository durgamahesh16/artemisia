package tech.artemisia.task.localhost

import java.io.File
import com.typesafe.config.Config
import tech.artemisia.task.settings.ConnectionHelper
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 6/22/16.
  */

case class SFTPConnection(host: String, port: Int = 22, username: String, password: Option[String] = None, pkey: Option[File] = None) {

}

object SFTPConnection extends ConnectionHelper {

  type T = SFTPConnection

  val configStructure =
    s"""|  "connection_[0]" = sftp_connection_name
        |  "connection_[1]" = {
        |     hostname = "sftp-host-name @required"
        |     port = "sftp-host-port @default(22)"
        |     username = "sftp-username @required"
        |     password = "sftppassword @optional(not required if key based authentication is used)"
        |     pkey = "/home/user/.ssh/id_rsa @optional(not required if username/password authentication is used)"
        |   }""".stripMargin


  val fieldDefinition = Seq(
    "hostname" -> "hostname of the sftp-server",
    "port" -> "sftp port number",
    "username" -> "username to be used for sftp connection",
    "password" -> "optional password for sftp connection if exists",
    "pkey" -> "optional private key to be used for the connection"
  )

  def apply(config: Config): SFTPConnection = {
    SFTPConnection(
      host = config.as[String]("hostname"),
      port = config.getAs[Int]("port").getOrElse(22),
      username = config.as[String]("username"),
      password = config.getAs[String]("password"),
      pkey = config.getAs[String]("pkey") map { new File(_) }
    )
  }



}


package tech.artemisia.task.localhost

import java.io.File
import tech.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config.Config

/**
  * Created by chlr on 6/22/16.
  */

case class SFTPConnection(host: String, port: Int, username: String, password: Option[String], pkey: Option[File]) {

}

object SFTPConnection {

  def apply(config: Config) = {
    SFTPConnection(
      host = config.as[String]("hostname"),
      port = config.as[Int]("port"),
      username = config.as[String]("username"),
      password = config.getAs[String]("password"),
      pkey = config.getAs[String]("pkey") map { new File(_) }
    )
  }

  def apply(name: String) = {

  }
}


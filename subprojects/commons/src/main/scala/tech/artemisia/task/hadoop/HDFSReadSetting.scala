package tech.artemisia.task.hadoop

import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.ConfigurationNode


/**
  *
  * @param location HDFS path
  * @param codec compression algorithm
  */
case class HDFSReadSetting(location: URI, codec: Option[String]) {

  codec foreach {
    x  => require(HDFSWriteSetting.allowedCodecs contains x.toLowerCase, s"$x is not a supported compression format")
  }

}

object HDFSReadSetting extends ConfigurationNode {

  val allowedCodecs = "gzip" :: "bzip2" :: "default" :: Nil

  override val defaultConfig: Config = ConfigFactory.empty()

  override val structure: Config = ConfigFactory parseString
    s"""
       | {
       |   location = /var/tmp/input.txt
       |   codec = gzip
       | }
     """.stripMargin

  override val fieldDescription: Map[String, Any] = Map(
    "location" -> "target HDFS path",
    "codec" -> ("compression format to use. The allowed codecs are" -> allowedCodecs)
  )
}
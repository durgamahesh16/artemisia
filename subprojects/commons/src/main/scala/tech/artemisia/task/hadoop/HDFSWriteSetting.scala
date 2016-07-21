package tech.artemisia.task.hadoop

import java.net.URI

import com.typesafe.config.{Config, ConfigFactory, ConfigMemorySize}
import tech.artemisia.task.ConfigurationNode
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  *
  * @param location hdfs path to write the data
  * @param replication replication factor for the file being writtten
  * @param blockSize HDFS block size of the target file
  * @param codec compression codec to use if any
  */
case class HDFSWriteSetting(location: URI, overwrite: Boolean = false, replication: Byte = 3, blockSize: Long = 67108864,
                            codec: Option[String] = None) {

  require(replication <= 5, "assert parameter cannot be greater than 5")
  codec foreach {
    x  => require(HDFSWriteSetting.allowedCodecs contains x.toLowerCase, s"$x is not a supported compression format")
  }

}

object HDFSWriteSetting extends ConfigurationNode[HDFSWriteSetting] {

  val allowedCodecs = "gzip" :: "bzip2" :: "default" :: Nil


  override val defaultConfig: Config = ConfigFactory parseString
    s"""
       | {
       |   replication = 3
       |   block-size = 64M
       |   overwrite = no
       | }
     """.stripMargin

  override val structure: Config = ConfigFactory parseString
    s"""
       | {
       |   location = "/user/hadoop/test"
       |   block-size = 120M
       |   overwrite = no
       |   codec = gzip
       |   replication = "2 @default(3) @info(allowed values 1 to 5)"
       | }
     """.stripMargin


  override val fieldDescription: Map[String, Any] = Map(
    "replication" -> "replication factor for the file. only values 1 to 5 are allowed",
    "block-size" -> "HDFS block size of the file",
    "overwrite" -> "overwrite target file it already exists",
    "codec" -> ("compression format to use. The allowed codecs are" -> allowedCodecs),
    "location" -> "target HDFS path"
  )


  def apply(config: Config): HDFSWriteSetting = {
    new HDFSWriteSetting(
      location = new URI(config.as[String]("location")),
      replication = config.as[Byte]("replication"),
      blockSize = config.as[ConfigMemorySize]("blockSize").toBytes,
      codec = config.getAs[String]("codec")
    )
  }

}


package tech.artemisia.task.hadoop

import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.ConfigurationNode
import tech.artemisia.util.CommandUtil
import tech.artemisia.util.HoconConfigUtil.Handler


/**
  *
  * @param location HDFS path
  * @param codec compression algorithm
  */
case class HDFSReadSetting(location: URI, codec: Option[String] = None, cliMode: Boolean = true,
                           cliBinary: String = "hadoop") {
  if (!cliMode) {
    codec foreach {
      x => require(HDFSWriteSetting.allowedCodecs contains x.toLowerCase, s"$x is not a supported compression format")
    }
  }

  def getCLIBinaryPath = {
    CommandUtil.getExecutablePath(this.cliBinary) match {
      case Some(binary) => binary
      case None => throw new RuntimeException(s"binary $cliBinary was not found in PATH")
    }
  }
}

object HDFSReadSetting extends ConfigurationNode[HDFSReadSetting] {

  val allowedCodecs = "gzip" :: "bzip2" :: "default" :: Nil

  override val defaultConfig: Config = ConfigFactory parseString
    """
      |{
      |  cli-mode = yes
      |  cli-binary = hadoop
      |}
    """.stripMargin

  override val structure: Config = ConfigFactory parseString
    s"""
       | {
       |   location = /var/tmp/input.txt
       |   codec = gzip
       |   cli-mode = "yes @default(yes)"
       |   cli-binary = "hdfs @default(hadoop) @info(use either hadoop or hdfs)"
       | }
     """.stripMargin

  override val fieldDescription: Map[String, Any] = Map(
    "location" -> "target HDFS path",
    "codec" -> ("compression format to use. This field is relevant only if local-cli is false. The allowed codecs are" -> allowedCodecs),
    "cli-mode" -> "boolean field to indicate if the local installed hadoop shell utility should be used to read data",
    "cli-binary" -> "hadoop binary to be used for reading. usually its either hadoop or hdfs. this field is relevant when cli-mode field is set to yes"
  )

  override def apply(config: Config): HDFSReadSetting = {
    new HDFSReadSetting(
      location = new URI(config.as[String]("location"))
      ,codec = config.getAs[String]("codec")
      ,cliMode = config.as[Boolean]("cli-mode")
      ,cliBinary = config.as[String]("cli-binary")
    )
  }
}
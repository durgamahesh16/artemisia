package tech.artemisia.task.database.teradata

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.ConfigurationNode
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.{CommandUtil, FileSystemUtil}
import tech.artemisia.util.HoconConfigUtil.Handler


/**
  * Created by chlr on 8/27/16.
  */



case class TDCHHadoopSetting(tdchJar: String, numMapper: Int = 10, queueName: String = "default", format: String = "textfile",
                             textSettings: TDCHTextFileSetting, libJars: Seq[String] = Nil, miscOptions: Map[String, String] = Map()) {

  import TDCHHadoopSetting._

  require(allowedFormats contains format, s"$format is not supported. ${allowedFormats.mkString(",")} are the only format supported")

  def commandArgs(export: Boolean, connection: DBConnection, jobType: String, otherSettings: Map[String, String]) = {

    val mainArguments = Map(
      "-url" -> s"jdbc:teradata://${connection.hostname}/database=${connection.default_database}",
      "-username" -> connection.username,
      "-password" -> connection.password,
      "-nummappers" -> numMapper.toString,
      "-jobtype" -> jobType,
      "-fileformat" -> format
    ) ++ getLibJars ++ getTextSettings ++ otherSettings

    val env = libJars match {
      case Nil => Map[String,String]()
      case x => Map("HADOOP_CLASSPATH" -> libJars.mkString(":"))
    }

    val command = CommandUtil.getExecutableOrFail("hadoop") :: "jar" :: tdchJar ::
      (if (export) "com.teradata.connector.common.tool.ConnectorImportTool" else "com.teradata.connector.common.tool.ConnectorExportTool") ::
    s"-Dmapred.job.queue.name=$queueName" :: Nil ++ (mainArguments flatMap { case (x,y) => x :: y :: Nil })

    command -> env

  }

  private val getTextSettings = {
   format match {
      case "textfile" if textSettings.quoting => Map(
            "-separator" -> ("\\u" + Integer.toHexString(textSettings.delimiter | 0x10000).substring(1)),
            "-escapedby" -> textSettings.escapedBy.toString,
            "-enclosedby" -> textSettings.quoteChar.toString
          ) ++
        textSettings.nullString.map(x => Map("-nullstring" -> x)).getOrElse(Map[String, String]())
      case "textfile" if !textSettings.quoting => Map(
        "-separator" -> ("\\u" + Integer.toHexString(textSettings.delimiter | 0x10000).substring(1))
      ) ++ textSettings.nullString.map(x => Map("-nullstring" -> x)).getOrElse(Map[String, String]())
      case _ => Map[String, String]()
    }
  }

  private def getLibJars = {
    libJars match {
      case Nil => Map[String, String]()
      case x =>  Map("-libjars" -> libJars.mkString(","))
    }
  }

}


object TDCHHadoopSetting extends ConfigurationNode[TDCHHadoopSetting] {

  private val allowedFormats = Seq("textfile", "avrofile", "rcfile", "orcfile", "sequenceFile")


  override val defaultConfig: Config = {
    val config = ConfigFactory parseString
      s"""
       | {
       |   num-mappers = 10
       |   queue-name = default
       |   format = textfile
       |   lib-jars = []
       |   misc-options = {}
       | }
     """.stripMargin
    config.withValue("text-setting", TDCHTextFileSetting.defaultConfig.root())
    }


  override def apply(config: Config): TDCHHadoopSetting= {
    TDCHHadoopSetting(
      config.as[String]("tdch-jar"),
      config.as[Int]("num-mappers"),
      config.as[String]("queue-name"),
      config.as[String]("format"),
      TDCHTextFileSetting(config.as[Config]("text-setting")),
      processLibJarsField(config.as[List[String]]("lib-jars")),
      config.asMap[String]("misc-options")
    )
  }

  override val structure: Config = {
    val config = ConfigFactory parseString
      s"""
       | {
       |   tdch-jar = "/path/teradata-connector.jar"
       |   num-mappers = "5 @default(10)"
       |   queue-name = "public @default(default)"
       |   format = "avrofile @default(default)"
       |   libjars = [
       |     "/path/hive/conf"
       |     "/path/hive/libs/*.jars"
       |   ]
       |   misc-options = {
       |     foo1 = bar1
       |     foo2 = bar2
       |   }
       | }
     """.stripMargin
    config.withValue("text-setting", TDCHTextFileSetting.structure.root())
    }


  override val fieldDescription: Map[String, Any] = Map(
    "tdch-jar" -> "path to tdch jar file",
    "num-mappers" -> "num of mappers to be used in the MR job",
    "queue-name" -> "scheduler queue where the MR job is submitted",
    "format" -> ("format of the file. Following are the allowed values" -> allowedFormats),
    "libjars" -> ("list of files and directories that will be added to libjars argument and set in HADOOP_CLASSPATH environment variable." +
      "Usually the hive conf and hive lib jars are added here. The path accept java glob pattern"),
    "text-setting" -> TDCHTextFileSetting.fieldDescription,
    "misc-options" -> "other TDHC arguments to be appended must be defined in this Config object"
  )

  /**
    * expand list of paths which uses java type glob pattern.
    *
    * @param paths
    * @return
    */
  private def processLibJarsField(paths: Seq[String]): Seq[String] = {
    paths.map(Paths.get(_))
      .flatMap(x => FileSystemUtil.expandPathToFiles(x))
      .map(_.toString)
  }
}



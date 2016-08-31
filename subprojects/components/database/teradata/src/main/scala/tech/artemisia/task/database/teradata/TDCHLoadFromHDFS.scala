package tech.artemisia.task.database.teradata

import java.sql.SQLException

import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.CommandUtil
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/26/16.
  */

class TDCHLoadFromHDFS(override val taskName: String, val dBConnection: DBConnection, val sourcePath: String
                       , val targetTable: String, val method: String = "batch.insert", val truncate: Boolean = false
                       , val tdchHadoopSetting: TDCHHadoopSetting)
  extends Task(taskName) {

  private val supportedFormats = Seq("avrofile", "textfile")

  private val supportedMethods = Seq("batch.insert", "internal.fastload")

  require(supportedMethods contains method, s"${supportedMethods.mkString(",")} ")

  require(supportedFormats contains tdchHadoopSetting.format, s"${supportedFormats.mkString(",")} ")

  /**
    * @todo detect error table overrides with errortablename argument
    */
  override protected[task] def setup(): Unit = {
    implicit val dbInterface = DBInterfaceFactory.getInstance(dBConnection)
    if (method == "batch.insert" && truncate) {
      dbInterface.execute(s"DELETE FROM $targetTable")
    } else {
      try { dbInterface.execute(s"DROP TABLE ${targetTable}_ERR_1") } catch { case th: SQLException => () }
      try { dbInterface.execute(s"DROP TABLE ${targetTable}_ERR_2") } catch { case th: SQLException => () }
      TeraUtils.dropRecreateTable(targetTable)
    }
  }

  override protected[task] def work(): Config = {
    val settings = Map("-sourcepaths" -> sourcePath, "-targettable" -> targetTable, "-method" -> method)
    val (command, env) = tdchHadoopSetting.commandArgs(export = false, connection = dBConnection, jobType = "hdfs", settings)
    val logParser = new TDCHTDLoadLogParser(System.out)
    CommandUtil.executeCmd(command = command, env = env, stderr = logParser)
    wrapAsStats {
      ConfigFactory.empty().withValue("rows", ConfigValueFactory.fromAnyRef(logParser.rowsLoaded.toString))
    }
  }

  override protected[task] def teardown(): Unit = {}

}

object TDCHLoadFromHDFS extends TaskLike {

  override val taskName: String = "TDCHLoadFromHDFS"

  override def paramConfigDoc: Config = {
    val config = ConfigFactory parseString
      """
       |{
       |  "dsn_[1]" = connection-name
       |	source-path =  "@required @info(source hdfs path)"
       |	target-table = "database.tablename @info(teradata tablename)"
       |  truncate = "yes @default(no)"
       |	method = "@allowed(batch.insert, internal.fastload) @default(batch.insert)"
       |}
     """.
        stripMargin
  config
    .withValue(""""dsn_[2]"""",DBConnection.structure(1025).root())
    .withValue("tdch-settings",TDCHHadoopSetting.structure.root())

  }


  override def defaultConfig: Config = {
    val config = ConfigFactory parseString
      """
       |{
       |	method = "batch.insert"
       |  truncate = no
       |}
     """.stripMargin
      config.withValue("tdch-setting", TDCHHadoopSetting.defaultConfig.root())
    }

  override def fieldDefinition: Map[String, AnyRef] = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "source-path" -> "hdfs path or hive tablename depending on the job-type defined",
    "target-table" -> "teradata tablename",
    "method" -> "defines whether to use fastload or normal jdbc insert for loading data to teradata",
    "truncate" -> "truncate target table before load",
    "tdch-setting" -> TDCHHadoopSetting.fieldDescription
  )

  /**
    * config based constructor for task
    *
    * @param name   a name for the task
    * @param config param config node
    */
  override def apply(name: String, config: Config): Task = {
    new TDCHLoadFromHDFS(name,
      DBConnection.parseConnectionProfile(config.as[ConfigValue]("dsn")),
      config.as[String]("source-path"),
      config.as[String]("target-table"),
      config.as[String]("method"),
      config.as[Boolean]("truncate"),
      TDCHHadoopSetting(config.as[Config]("tdch-setting"))
    )
  }

  override val info: String =
    """
      | Loads data from HDFS path to Teradata.
    """.stripMargin

  override val desc: String =
    """
      | Loads data from HDFS path to Teradata. The hadoop task nodes directly connect to Teradata nodes (AMPs)
      | and the data from hadoop is loaded to Teradata with map reduce jobs processing the data in hadoop and transferring
      | them over to Teradata. Preferred method of transferring large volume of data between Hadoop and Teradaata
    """.stripMargin




}

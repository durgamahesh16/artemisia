package tech.artemisia.task.database.teradata.tdch

import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import tech.artemisia.core.AppLogger._
import tech.artemisia.task.database.DBUtil
import tech.artemisia.task.hadoop.hive.HiveCLIInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.{CommandUtil, Util}

/**
  * Created by chlr on 9/3/16.
  */
class TDCHExtract(override val taskName: String, val dBConnection: DBConnection, val sourceType: String, val source: String,
                  val targetType: String = "hdfs", val target: String,  val splitBy: String = "split.hash",
                  val truncate: Boolean = false, val tdchHadoopSetting: TDCHSetting) extends Task(taskName) {

  import TDCHExtract._

  require(allowedSplitBys contains splitBy, s"split by $splitBy is not allowed. supported values " +
    s"are ${allowedSplitBys.mkString(",")}")

  require(supportedSourceType contains sourceType, s"source-type $sourceType is not allowed. supported values " +
    s"are ${supportedSourceType.mkString(",")}")

  require(supportedTargetType contains targetType, s"target-type $targetType is not allowed. supported values " +
    s"are ${supportedTargetType.mkString(",")}")

  protected val logStream = new TDCHLogParser(System.out)


  /**
    * generate command parameters based on the given source-type and target-type
    * @return
    */
  private[teradata] def sourceTargetParams = {
    val sourceSettings = targetType match {
      case "hdfs" => Map("-jobtype" -> "hdfs", "-targetpaths" -> target)
      case "hive" => Map("-jobtype" -> "hive", "-targettable" -> target)
    }
    val targetSettings = sourceType match {
      case "table" => Map("-sourcetable" -> source)
      case "query" => Map("-sourcequery" -> source)
    }
    sourceSettings ++ targetSettings
  }


  private val deleteHadoopDir = tdchHadoopSetting.hadoopBin :: "dfs" :: "-rm" :: "-r" :: target :: Nil

  private val dropRecreateTable = {
    val (database, tablename) = DBUtil.parseTableName(target)
    val uuid = Util.getUUID.replace("-","")
    val tempTable = database.map(x => s"$x.$uuid").getOrElse(uuid)
    s"""
       | CREATE TABLE $tempTable LIKE $target;
       | DROP TABLE $target;
       | ALTER TABLE $tempTable RENAME TO $target
     """.stripMargin
  }


  override protected[task] def setup(): Unit = {
    if (truncate) {
      targetType match {
        case "hdfs" =>
          debug(s"removing target hdfs directory")
          CommandUtil.executeCmd(deleteHadoopDir)
        case "hive" =>
          val cli = new HiveCLIInterface(tdchHadoopSetting.hiveBin)
          debug(s"dropping and recreating hive table")
          cli.execute(dropRecreateTable, taskName, printSQL = false)
      }
    }
  }

  override protected[task] def work(): Config = {
    val methodMap = Map (
      "hash" -> "split.by.hash",
      "partition" -> "split.by.partition",
      "amp" -> "split.by.amp"
    )
    val settings: Map[String, String] = sourceTargetParams + ("-method" -> methodMap(splitBy))
    val (command, env) = tdchHadoopSetting.commandArgs(export = true, connection = dBConnection, settings)
    CommandUtil.executeCmd(command = command, env = env, stderr = logStream, obfuscate = Seq(command.indexOf("-password")+2))
    wrapAsStats {
      ConfigFactory.empty().withValue("rows", ConfigValueFactory.fromAnyRef(logStream.rowsLoaded.toString))
    }
  }

  override protected[task] def teardown(): Unit = {}

}

object TDCHExtract extends TaskLike {

  val allowedSplitBys = Seq("hash", "partition", "amp", "value")
  val supportedSourceType = Seq("table", "query")
  val supportedTargetType = Seq("hdfs", "hive")

  override val taskName: String = "TDCHExtract"

  override def paramConfigDoc: Config = {
    val config = ConfigFactory parseString
      s"""
        |{
        |  "dsn_[1]" = connection-name
        |  source-type = "@default(table) @allowed(table, query)"
        |	 source =  "@required @info(tablename or sql query)"
        |  target-type = "@default(hdfs) @allowed(hive, hdfs)"
        |	 target = "@required @info(hdfs path or hive tablename)"
        |	 split-by = "@allowed(${allowedSplitBys.mkString(",")}) @default(hash)"
        |}
      """.
        stripMargin
    config
      .withValue(""""dsn_[2]"""",DBConnection.structure(1025).root())
      .withValue("tdch-settings",TDCHSetting.structure.root())
  }

  /**
    *
    */
  override def defaultConfig: Config = {
    val config = ConfigFactory parseString
      """
        |{
        |  source-type = table
        |  target-type = hdfs
        |	 split = hash
        |}
      """.stripMargin
    config.withValue("tdch-setting", TDCHSetting.defaultConfig.root())
  }


  override def fieldDefinition: Map[String, AnyRef] = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "source-type" -> "source can be either a table or a sql query. The allowed values for this field are (**table**, **query**)",
    "source" -> "defines the source table or query depending on the defined source type",
    "target-type" -> "defines if the target is a HDFS path or a Hive table",
    "split-by" -> "defines how the source table/query is split. allowed values being hash, partition, amp",
    "truncate" -> "if target is HDFS directory it is deleted. If target is a hive table it is dropped and recreated",
    "tdch-setting" -> TDCHSetting.fieldDescription
  )

  override def apply(name: String, config: Config): Task = {
    new TDCHExtract(
      name,
      DBConnection.parseConnectionProfile(config.as[ConfigValue]("dsn")),
      config.as[String]("source-type"),
      config.as[String]("source"),
      config.as[String]("target-type"),
      config.as[String]("target"),
      config.as[String]("split-by"),
      config.as[Boolean]("truncate"),
      TDCHSetting(config.as[Config]("tdch-setting"))
    )
  }

  override val info: String = "Extract data from Teradata to HDFS/Hive"

  override val desc: String =
    """
      | Extract data from Teradata to HDFS/Hive. The hadoop task nodes directly connect to Teradata nodes (AMPs)
      | and the data from Teradata is loaded to HDFS/Hive with map reduce jobs processing the data in hadoop and extracting
      | them from Teradata. This is the preferred method of transferring large volume of data between Hadoop and Teradata.
      |
      | This requires TDCH library to be installed on the local machine. The target can be either a random HDFS directory
      | or a Hive table. The source can be either a Teradata table or a query. The task sports a truncate option which will
      | delete the contents of target HDFS directory or truncate the data in the Hive table depending on the target-type
      | selected. The **split-by** fields decides how the data is distributed and parallelized. The default value for
      | this field is *hash*.
      |
      | To use hive as a target the field **tdch-settings.libjars** must be set with all the
      |
      | * Hive conf dir
      | * Hive library jars (jars in lib directory of hive)
      |
      | The **tdch-settings.libjars** field supports java style glob pattern. so for eg if hive lib path is located at
      |  `/var/path/hive/lib` and to add all the jars in the lib directory to the **tdch-settings.libjars** field one can
      |  use java style glob patterns such as `/var/path/hive/lib/*.jar`. so the most common value for **tdch-settings.libjars**
      |  will be like `libjars = ["/var/path/hive/conf", "/var/path/hive/lib/*.jar"]`.
      |
      |
      | If you want to set any specific TDCH command line argument that is not available in this task param such as
      | `targettimestampformat`, `usexviews` etc, you can use the  **tdch-settings.misc-options** field to defined these
      | arguments and values. for eg the below config object would effectively result in arguments `--foo bar --hello world`
      | added to the TDCH CLI command.
      |
      |
      |           misc-options = {
      |              foo = bar,
      |              hello = world
      |           }
      |
    """.stripMargin

}

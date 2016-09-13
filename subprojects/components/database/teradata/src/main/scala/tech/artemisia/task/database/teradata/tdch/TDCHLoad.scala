package tech.artemisia.task.database.teradata.tdch

import java.sql.SQLException

import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import tech.artemisia.task.database.teradata._
import tech.artemisia.task.database.{DBInterface, DBUtil}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.CommandUtil
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/26/16.
  */

class TDCHLoad(override val taskName: String, val dBConnection: DBConnection, val sourceType: String = "hdfs",
               val source: String, val targetTable: String, val method: String = "batch.insert",
               val truncate: Boolean = false, val tdchHadoopSetting: TDCHSetting)
  extends Task(taskName) {

  val dbInterface: DBInterface = DBInterfaceFactory.getInstance(dBConnection)

  protected val logStream = new TDCHLogParser(System.out)


  private val supportedMethods = Seq("batch.insert", "internal.fastload")

  private val supportedSourceTypes = Seq("hive", "hdfs")

  require(supportedMethods contains method, s"${supportedMethods.mkString(",")} ")

  require(supportedSourceTypes contains sourceType, s"source-type $sourceType is not supported. " +
    s"supported types are (${supportedSourceTypes.mkString(",")})")


  /**
    * @todo detect error table overrides with errortablename argument
    */
  override protected[task] def setup(): Unit = {
    if (method == "batch.insert" && truncate) {
      dbInterface.execute(s"DELETE FROM $targetTable", printSQL = false)
    }

    else if(method == "internal.fastload") {
      try { dbInterface.execute(s"DROP TABLE ${targetTable}_ERR_1", printSQL = false) } catch { case th: SQLException => () }
      try { dbInterface.execute(s"DROP TABLE ${targetTable}_ERR_2", printSQL = false) } catch { case th: SQLException => () }
      TeraUtils.dropRecreateTable(targetTable)(dbInterface)
    }
  }

  override protected[task] def work(): Config = {
    val sourceTypeConfigMap = Map (
      "hive" -> "sourcetable",
      "hdfs" -> "sourcepaths"
    )
    val settings: Map[String, String] = Map("-targettable" -> targetTable, "-method" -> method) ++ TDCHLoad.generateSourceParams(source, sourceType)
    val (command, env) = tdchHadoopSetting.commandArgs(export = false, connection = dBConnection, settings)
    CommandUtil.executeCmd(command = command, env = env, stderr = logStream, obfuscate = Seq(command.indexOf("-password")+2))
    wrapAsStats {
      ConfigFactory.empty().withValue("rows", ConfigValueFactory.fromAnyRef(logStream.rowsLoaded.toString))
    }
  }

  override protected[task] def teardown(): Unit = {}

}

object TDCHLoad extends TaskLike {

  override val taskName: String = "TDCHLoad"

  def generateSourceParams(source: String, sourceType: String) = {

    def hiveTableCommand(tableName: String) = {
      DBUtil.parseTableName(tableName) match {
        case (Some(database), table) => Map("-sourcedatabase" -> database, "-sourcetable" -> table)
        case (None, table) =>  Map("-sourcetable" -> table)
      }
    }

    sourceType match {
      case "hdfs" => Map("-jobtype" -> "hdfs", "-sourcepaths" -> source)
      case "hive" => Map("-jobtype" -> "hive") ++ hiveTableCommand(source)
    }
  }

  override def paramConfigDoc: Config = {
    val config = ConfigFactory parseString
      """
       |{
       |  "dsn_[1]" = connection-name
       |  source-type = "hive @defualt(hdfs) @allowed(hive, hdfs)"
       |	source =  "@required @info(hdfs path or hive table)"
       |	target-table = "database.tablename @info(teradata tablename)"
       |  truncate = "yes @default(no)"
       |	method = "@allowed(batch.insert, internal.fastload) @default(batch.insert)"
       |}
     """.
        stripMargin
  config
    .withValue(""""dsn_[2]"""",DBConnection.structure(1025).root())
    .withValue("tdch-settings",TDCHSetting.structure.root())

  }


  override def defaultConfig: Config = {
    val config = ConfigFactory parseString
      """
       |{
       |  source-type = hdfs
       |	method = "batch.insert"
       |  truncate = no
       |}
     """.stripMargin
      config.withValue("tdch-setting", TDCHSetting.defaultConfig.root())
    }

  override def fieldDefinition: Map[String, AnyRef] = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "source-type" -> "type of the source. currently hive and hdfs are the allowed values",
    "source" -> "hdfs path or hive tablename depending on the job-type defined",
    "target-table" -> "teradata tablename",
    "method" -> "defines whether to use fastload or normal jdbc insert for loading data to teradata",
    "truncate" -> "truncate target table before load",
    "tdch-setting" -> TDCHSetting.fieldDescription
  )

  /**
    * config based constructor for task
    *
    * @param name   a name for the task
    * @param config param config node
    */
  override def apply(name: String, config: Config): Task = {
    new TDCHLoad(name,
      DBConnection.parseConnectionProfile(config.as[ConfigValue]("dsn")),
      config.as[String]("source-type"),
      config.as[String]("source"),
      config.as[String]("target-table"),
      config.as[String]("method"),
      config.as[Boolean]("truncate"),
      TDCHSetting(config.as[Config]("tdch-setting"))
    )
  }

  override val info: String = "Loads data from HDFS/Hive  into Teradata"

  override val desc: String =
    """
      | Loads data from HDFS/Hive to Teradata. The hadoop task nodes directly connect to Teradata nodes (AMPs)
      | and the data from hadoop is loaded to Teradata with map reduce jobs processing the data in hadoop and transferring
      | them over to Teradata. Preferred method of transferring large volume of data between Hadoop and Teradata
    """.stripMargin


}

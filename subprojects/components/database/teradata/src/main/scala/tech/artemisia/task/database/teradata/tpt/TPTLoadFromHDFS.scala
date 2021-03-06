package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.Config
import tech.artemisia.task.Task
import tech.artemisia.task.database.teradata.TeraUtils
import tech.artemisia.task.hadoop.{HDFSCLIReader, HDFSReadSetting}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * TPTLoadFromHDFS lets you load file to Teradata using TPT.
  * It supports Load Operator and Stream operator depending on the mode.
  * This is abstract class. you can instantiate it using the apply method
  * as factory. This is because the TPTLoadSetting has to be preprocessed
  * when the mode is set to '''auto'''.
  *
  * @param taskName name of the task
  * @param tableName target table name
  * @param hdfsReadSetting hdfs setting
  * @param connectionProfile database connection profile
  * @param loadSetting load settings
  */
abstract class TPTLoadFromHDFS(override val taskName: String
                      ,override val tableName: String
                      ,val hdfsReadSetting: HDFSReadSetting
                      ,override val connectionProfile: DBConnection
                      ,override val loadSetting: TPTLoadSetting) extends
  TPTLoad(taskName, tableName, hdfsReadSetting.location, connectionProfile, loadSetting) {


  assert(hdfsReadSetting.cliMode, "cli mode must be true for this task. set cli-mode field to true in the task")

  /**
    *  tpt script generator
    */
  override val scriptGenerator = BaseTPTLoadScriptGen.create(tptLoadConfig, loadSetting, connectionProfile)

  /**
    * get reader Future. this Future will launch a thread
    *
    * @return
    */
  override lazy val readerFuture = {
    val textCmd = s"${hdfsReadSetting.cliBinary} dfs -text $location > $dataPipe"
    Future {
      val ret = executeShellCommand(textCmd)
      assert(ret == 0, s"command $textCmd failed with return code of $ret")
    }
  }

}

object TPTLoadFromHDFS extends TPTTaskHelper {

  override val taskName: String = "TPTLoadFromHDFS"

  override val fieldDefinition = super.fieldDefinition - "location" + ("hdfs" -> HDFSReadSetting.fieldDescription)

  override val defaultConfig = super.defaultConfig
    .withoutPath("location")
    .withValue("hdfs", HDFSReadSetting.defaultConfig.root())

  override val paramConfigDoc = super.paramConfigDoc
    .withoutPath("location")
    .withValue("hdfs", HDFSReadSetting.structure.root())

  override def apply(name: String, config: Config): Task = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = TPTLoadSetting(config.as[Config]("load"))
    val location = HDFSReadSetting(config.as[Config]("hdfs"))
    TPTLoadFromHDFS(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  /**
    * creates an instance of TPTLoadFromHDFS.
    *
    * @param taskName name of the task
    * @param tableName target table name
    * @param hdfsReadSetting hdfs setting
    * @param connectionProfile database connection profile
    * @param loadSetting load settings
    * @return
    */
  def apply(taskName: String, tableName: String, hdfsReadSetting: HDFSReadSetting, connectionProfile: DBConnection,
            loadSetting: TPTLoadSetting) = {
    val cli = new HDFSCLIReader(hdfsReadSetting.cliBinary)
    val loadSize = cli.getPathSize(hdfsReadSetting.location)
    val optimizedLoadSetting = TeraUtils.autoTuneLoadSettings(loadSize ,loadSetting)
    new TPTLoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile ,optimizedLoadSetting) {
      override protected val loadDataSize: Long = loadSize
    }
  }

  override val info: String = "Load data to Teradata from HDFS"

  override val desc: String =
    """
      | Load data from a HDFS filesystem to Teradata. This task is supported only in POSIX OS like Linux/Mac OS X.
      |  This task also expects the TPT binary installed in the local machine. It supports two mode of operations.
      |
      |  * **default**: This uses TPT Stream operator to load data.
      |  * **fastload**: This uses TPT load operator to load data.
      |
      |  To use either of the modes set **load.mode** property to *default*, *fastload* or *auto*.
      |  when the mode is set to *auto*, one of the two modes of *default* or *fastload* is automatically selected
      |  depending on the size of the data to be loaded. The property **load.bulk-threshold** defines the threshold
      |  for selecting the *default* and *fastload* mode. for eg if **load.bulk-threshold** is defined as 50M
      |  (50 Megabytes) any file(s) whose total size is lesser than 50M will be loaded by *default* mode and any file(s)
      |  larger than this threshold will be loaded via the *fastload* mode.
      |
      |  The truncate option internally tries to delete the target table but if the target table has a fastload lock
      |  on the table the target table is dropped and re-created.
    """.stripMargin

}

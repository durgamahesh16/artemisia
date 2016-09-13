package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.Config
import tech.artemisia.task.Task
import tech.artemisia.task.hadoop.HDFSReadSetting
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by chlr on 9/13/16.
  */

class TPTLoadFromHDFS(override val taskName: String
                      ,override val tableName: String
                      ,val hdfsReadSetting: HDFSReadSetting
                      ,override val connectionProfile: DBConnection
                      ,override val loadSetting: TPTLoadSetting) extends TPTLoad(taskName, tableName, hdfsReadSetting.location, connectionProfile, loadSetting) {


  assert(hdfsReadSetting.cliMode, "cli mode must be true for this task. set cli-mode field to true in the task")


  /**
    *  tpt script generator
    */
  override val scriptGenerator = new TPTLoadScriptGenerator(tptLoadConfig, loadSetting, connectionProfile)

  /**
    * get reader Future. this Future will launch a thread
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
    new TPTLoadFromHDFS(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  override val info: String = "Load data to Teradata from HDFS"

  override val desc: String =
    """
      |
    """.stripMargin

}

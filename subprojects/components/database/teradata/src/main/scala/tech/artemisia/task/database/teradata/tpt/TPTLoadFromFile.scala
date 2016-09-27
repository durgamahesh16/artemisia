package tech.artemisia.task.database.teradata.tpt

import java.net.URI
import com.typesafe.config.Config
import tech.artemisia.task.Task
import tech.artemisia.task.database.teradata.TeraUtils
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.BashUtil
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.core.AppLogger._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try


/**
  * TPTLoadFromFile lets you load file to Teradata using TPT.
  * It supports Load Operator and Stream operator depending on the mode.
  * This is abstract class. you can instantiate it using the apply method
  * as factory. This is because the TPTLoadSetting has to be preprocessed
  * when the mode is set to '''auto'''.
  *
  * @param taskName name of the task
  * @param tableName target table name
  * @param location location of the file(s) to load.
  * @param connectionProfile database connection profile
  * @param loadSetting load settings
  */
abstract class TPTLoadFromFile(override val taskName: String
                     ,override val tableName: String
                     ,override val location: URI
                     ,override val connectionProfile: DBConnection
                     ,override val loadSetting: TPTLoadSetting)
  extends TPTLoad(taskName, tableName, location, connectionProfile, loadSetting) {

    /**
      *  tpt script generator
      */
    override val scriptGenerator = BaseTPTLoadScriptGen.create(tptLoadConfig, loadSetting, connectionProfile)

    /**
     * get reader Future. this Future will launch a thread
     * @return
     */
    override lazy val readerFuture = {
      val textCmd = s"cat $location > $dataPipe"
      Future {
        val ret = executeShellCommand(textCmd)
        assert(ret == 0, s"command $textCmd failed with return code of $ret")
      }
    }

    override def setup() = {
      // The error table has to be truncated because when the source file(s) is empty
      // the entire TPT launch is skipped. but if there was an error table from a
      // previous run with data entries in it say for example 3 records. then the output config would
      // erroneously consider the source had 3 records and all were sent to the error table.
      // so cleaning up of the error table is necessary.
      debug("truncating error table")
      Try(dbInterface.execute(s"DELETE FROM ${tableName}_ET;", printSQL = false))
      super.setup()
    }

}

object TPTLoadFromFile extends TPTTaskHelper {

  override val taskName: String = "TPTLoadFromFile"

  override def apply(name: String, config: Config): Task = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = TPTLoadSetting(config.as[Config]("load"))
    val location = new URI(config.as[String]("location"))
    TPTLoadFromFile(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  /**
    * create an Instance of TPTLoadFromFile.
    * The TPTLoadSetting is transformed and optimized if auto mode is set.
    * @param taskName
    * @param tableName
    * @param location
    * @param connectionProfile
    * @param loadSetting
    */
  def apply(taskName: String, tableName: String, location: URI, connectionProfile: DBConnection,
            loadSetting: TPTLoadSetting) = {
    val loadSize = BashUtil.pathSize(location.toString)
    val optimizedLoadSetting = TeraUtils.autoTuneLoadSettings(loadSize ,loadSetting)
    new TPTLoadFromFile(taskName, tableName, location, connectionProfile, optimizedLoadSetting) {
      override protected val loadDataSize: Long = loadSize
    }
  }

  override val info: String = "Load data from Local file to Teradata using TPT"

  override val desc: String =
    """
      |
    """.stripMargin

}

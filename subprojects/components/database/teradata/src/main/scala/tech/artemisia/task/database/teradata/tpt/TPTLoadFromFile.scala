package tech.artemisia.task.database.teradata.tpt

import java.net.URI
import com.typesafe.config.Config
import tech.artemisia.task.Task
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
 * Created by chlr on 9/12/16.
 */
class TPTLoadFromFile(override val taskName: String
                     ,override val tableName: String
                     ,override val location: URI
                     ,override val connectionProfile: DBConnection
                     ,override val loadSetting: TPTLoadSetting) extends TPTLoad(taskName, tableName, location, connectionProfile, loadSetting) {

    /**
      *  tpt script generator
      */
    override val scriptGenerator = new TPTLoadScriptGenerator(tptLoadConfig, loadSetting, connectionProfile)

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

}

object TPTLoadFromFile extends TPTTaskHelper {

  override val taskName: String = "TPTLoadFromFile"

  override def apply(name: String, config: Config): Task = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = TPTLoadSetting(config.as[Config]("load"))
    val location = new URI(config.as[String]("location"))
    new TPTLoadFromFile(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  override val info: String = "Load data from Local file to Teradata using TPT"

  override val desc: String =
    """
      |
    """.stripMargin

}

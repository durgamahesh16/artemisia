package tech.artemisia.task.database.teradata

import java.io.File
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import tech.artemisia.task.database.{DBUtil, LoadTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskContext}
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.FileSystemUtil.{FileEnhancer, _}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
  * Created by chlr on 9/9/16.
  */

class TPTLoadFromFile(override val taskName: String
                     ,val tableName: String
                     ,val location: URI
                     ,val connectionProfile: DBConnection
                     ,val loadSetting: TPTLoadSetting)  extends Task(taskName) {


  implicit protected val dbInterface = DBInterfaceFactory.getInstance(connectionProfile)

  lazy protected val tbuildBin = getExecutableOrFail("tbuild")

  protected val dataPipe = joinPath(TaskContext.workingDir.toString, "input.pipe")

  protected val tptScriptFile = this.getFileHandle("load.scr")

  private val tptCheckpointDir = {
    val dir = new File(joinPath(TaskContext.workingDir.toString, "tpt_checkpoint"))
    dir.mkdirs()
    FileUtils.cleanDirectory(dir)
    dir.toString
  }

  override protected[task] def setup(): Unit = {
    assert(TeraUtils.detectTPTRun(tableName) == Nil, s"detected TPT job(s) already running for the table " +
      s"${TeraUtils.detectTPTRun(tableName).mkString(",")}. try again after sometime")
    if (loadSetting.truncate) {
      TeraUtils.truncateElseDrop(tableName)
    }
    createNamedPipe(dataPipe)
    val (database, table) = DBUtil.parseTableName(tableName) match {
      case (Some(x),y) => x -> y
      case (None, y) => connectionProfile.default_database -> y
    }
    val scriptGenerator = new TPTLoadScriptGenerator(
      TPTLoadConfig(database, table, TaskContext.workingDir.toString, "input.pipe")
        ,loadSetting
        ,connectionProfile
    )
    tptScriptFile <<= scriptGenerator.tptScript
  }

  override protected[task] def work(): Config = {
    val textCmd = s"cat $location > $dataPipe"
    val tptCommand = Seq(tbuildBin, "-f", tptScriptFile.toString, "-h", "128M", "-j", tableName,"-r",
      TaskContext.workingDir.toString, "-r", tptCheckpointDir, "-R", "0", "-z", "0")
    val (writer, reader) = Future{executeShellCommand(textCmd)} -> Future{executeCmd(tptCommand)}
    for(writerResult <- writer; readerResult <- reader)

    ConfigFactory.empty()
  }

  override protected[task] def teardown(): Unit = {
    new File(dataPipe).delete()
  }
}

object TPTLoadFromFile extends LoadTaskHelper {

  override val taskName: String = "TPTLoadFromFile"

  override val paramConfigDoc =  super.paramConfigDoc.withValue("load",TPTLoadSetting.structure.root())

  override def defaultConfig: Config = ConfigFactory.empty().withValue("load", TPTLoadSetting.defaultConfig.root())

  override val fieldDefinition = super.fieldDefinition ++ Map("load" -> TPTLoadSetting.fieldDescription )

  override def apply(name: String, config: Config): Task = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = TPTLoadSetting(config.as[Config]("load"))
    val location = new URI(config.as[String]("location"))
    new TPTLoadFromFile(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  override val info: String = "Load data from Local Filesystem into Teradata using TPT"

  override val desc: String =
    """
      |
    """.stripMargin

  /**
    * undefined default port
    */
  override def defaultPort: Int = 1025

  override def supportedModes: Seq[String] = Seq("fastload", "default", "auto")

}

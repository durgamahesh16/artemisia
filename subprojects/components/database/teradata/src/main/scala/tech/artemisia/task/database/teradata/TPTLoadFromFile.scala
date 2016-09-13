package tech.artemisia.task.database.teradata

import java.io.File
import java.net.URI
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import tech.artemisia.core.AppLogger._
import tech.artemisia.task.database.{DBUtil, LoadTaskHelper}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskContext}
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}


/**
  * Created by chlr on 9/9/16.
  */

/**
 *
 * @param taskName name of the task
 * @param tableName target table name
 * @param location location of the file(s) to load.
 * @param connectionProfile database connection profile
 * @param loadSetting
 */
class TPTLoadFromFile(override val taskName: String
                     ,val tableName: String
                     ,val location: URI
                     ,val connectionProfile: DBConnection
                     ,val loadSetting: TPTLoadSetting)  extends Task(taskName) {

  implicit protected val dbInterface = DBInterfaceFactory.getInstance(connectionProfile)

  lazy protected val tbuildBin = getExecutableOrFail("tbuild")

  lazy protected val twbKillBin = getExecutableOrFail("twbkill")

  protected val dataPipe = joinPath(TaskContext.workingDir.toString, "input.pipe")

  protected val tptScriptFile = this.getFileHandle("load.scr")

  protected val logParser = new TPTLoadLogParser(System.out)


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
    val combinedFuture = TPTLoadFromFile.monitor(readerFuture, writerFuture)
    Await.result(combinedFuture, Duration.Inf)
    wrapAsStats {
      ConfigFactory.empty()
        .withValue("sent", ConfigValueFactory.fromAnyRef(logParser.rowsSent))
        .withValue("applied", ConfigValueFactory.fromAnyRef(logParser.rowsApplied))
        .withValue("err_table1", ConfigValueFactory.fromAnyRef(logParser.rowsErr1))
        .withValue("err_table2", ConfigValueFactory.fromAnyRef(logParser.rowsErr2))
        .withValue("duplicate", ConfigValueFactory.fromAnyRef(logParser.rowsDuplicate))
        .withValue("err_file", ConfigValueFactory.fromAnyRef(logParser.errorFileRows))
    }
  }

  override protected[task] def teardown(): Unit = {
    if (logParser.jobId != null) {
     TeraUtils.detectTPTRun(logParser.jobId) match {
       case jobId :: Nil =>
         debug(s"attempting to kill tpt job ${logParser.jobId}")
         TeraUtils.killTPTJob(twbKillBin,logParser.jobId)
       case _ => ()
     }
    }
    new File(dataPipe).delete()
  }


  def readerFuture = {
    val textCmd = s"cat $location > $dataPipe"
    Future {
      val ret = executeShellCommand(textCmd)
      assert(ret == 0, s"command $textCmd failed with return code of $ret")
    }
  }

  def writerFuture = {
    val tptCommand = Seq(tbuildBin, "-f", tptScriptFile.toString, "-h", "128M", "-j", tableName,"-r",
      TaskContext.workingDir.toString, "-r", tptCheckpointDir, "-R", "0", "-z", "0")
    Future {
      val ret = executeCmd(tptCommand, stdout = logParser)
      assert(ret == 0, s"command ${tptCommand.mkString(" ")} failed with return code $ret")
    }
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


  /**
    * takes in reader Future and writer Future and provides a new Future that holds the tuple
    * of the reader and writer Future. this combined future fails fast. ie if either the reader
    * or the writer future fails the resultant future also fails immediately and doesnt wait for the
    * other future to resolve.
    *
    * @param readerFuture
    * @param writerFuture
    * @return
    */
  def monitor(readerFuture: Future[Unit], writerFuture: Future[Unit]): Future[(Unit,Unit)] = {
    val promise = Promise[(Unit,Unit)]
    readerFuture onFailure { case th if !promise.isCompleted =>  promise.failure(th) }
    writerFuture onFailure { case th if !promise.isCompleted => promise.failure(th) }
    val res = readerFuture zip writerFuture
    promise.completeWith(res).future
  }


}

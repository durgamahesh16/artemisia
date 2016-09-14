package tech.artemisia.task.database.teradata.tpt

import java.io.{ByteArrayOutputStream, File}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import tech.artemisia.core.AppLogger._
import tech.artemisia.task.database.DBUtil
import tech.artemisia.task.database.teradata._
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskContext}
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.FileSystemUtil._

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
 * @param loadSetting load settings
 */
abstract class TPTLoad(override val taskName: String
                     ,val tableName: String
                     ,val location: URI
                     ,val connectionProfile: DBConnection
                     ,val loadSetting: TPTLoadSetting) extends Task(taskName) {

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

  protected val tptLoadConfig =  {
    val (database, table) = DBUtil.parseTableName(tableName) match {
      case (Some(x),y) => x -> y
      case (None, y) => connectionProfile.default_database -> y
    }
    TPTLoadConfig(database, table, TaskContext.workingDir.toString, "input.pipe")
  }

  /**
   *
   */
  protected val scriptGenerator: TPTScriptGenerator

  /**
   * writer future. this is a Future of type Unit that launches the TPT script on a separate thread.
   */
  protected val readerFuture: Future[Unit]

  /**
   * writer future. this is a Future of type Unit that launches the TPT script on a separate thread.
   */
  protected lazy val writerFuture = {
    val tptCommand = Seq(tbuildBin, "-f", tptScriptFile.toString, "-h", "128M", "-j", tableName,"-r",
      TaskContext.workingDir.toString, "-r", tptCheckpointDir, "-R", "0", "-z", "0", "-o")
    Future {
      val ret = executeCmd(tptCommand, stdout = logParser)
      assert(ret == 0, s"command ${tptCommand.mkString(" ")} failed with return code $ret")
    }
  }

  override protected[task] def setup(): Unit = {
    assert(TPTLoad.detectTPTRun(tableName) == Nil, s"detected TPT job(s) already running for the table " +
      s"${TPTLoad.detectTPTRun(tableName).mkString(",")}. try again after sometime")
    if (loadSetting.truncate) {
      TeraUtils.truncateElseDrop(tableName)
    }
    createNamedPipe(dataPipe)
    tptScriptFile <<= scriptGenerator.tptScript
  }

  override protected[task] def work(): Config = {
    val combinedFuture = TPTLoad.monitor(readerFuture, writerFuture)
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
     TPTLoad.detectTPTRun(logParser.jobId) match {
       case jobId :: Nil =>
         debug(s"attempting to kill tpt job ${logParser.jobId}")
         killTPTJob(logParser.jobId)
       case _ => ()
     }
    }
    if (logParser.jobLogFile != null)
      debug(s"run tlogview -l ${logParser.jobLogFile} to view job output")
    new File(dataPipe).delete()
  }

  /**
    *
    * @param jobName
    */
  def killTPTJob(jobName: String) = {
    val cmd = Seq(twbKillBin, jobName)
    executeCmd(cmd)
  }


}

object TPTLoad {

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
    val promise = Promise[(Unit, Unit)]()
    readerFuture onFailure { case th if !promise.isCompleted =>  promise.failure(th) }
    writerFuture onFailure { case th if !promise.isCompleted => promise.failure(th) }
    val res = readerFuture zip writerFuture
    promise.completeWith(res).future
  }

  /**
    *
    * This is implemented by running twbstat command and parsing the output.
    * @param jobName tpt job name
    * @return
    */
  def detectTPTRun(jobName: String): Seq[String] = {
    val stream = new ByteArrayOutputStream()
    assert(executeCmd(Seq("twbstat"), stdout =stream) == 0, "twbstat command failed. ensure TPT is properly installed")
    val content = new String(stream.toByteArray)
    val rgx = s"$jobName-[\\d]+".r
    content.split(System.lineSeparator())
      .map(_.trim)
      .filter(rgx.findFirstMatchIn(_).isDefined)
  }


}



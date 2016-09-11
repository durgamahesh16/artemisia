package tech.artemisia.task

import java.io.File
import java.nio.file.Path

import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.Keywords
import tech.artemisia.util.FileSystemUtil.joinPath
import tech.artemisia.util.HoconConfigUtil.Handler


/**
 * Created by chlr on 3/7/16.
 */


/**
 * This object is used to hold contextual information required for Task execution.
 * properties such as common working directory for all tasks which doesn't qualify to an task attribute but still required for task execution goes here.
 */
private[artemisia] object TaskContext {

  private var preferredWorkingDir: Option[Path] = None


  /**
   * the entire payload. This field exists here to facilitate substitution during very late stage
   * like processing a sql_file/script_file
   */
  var payload: Config = ConfigFactory.empty

  /**
   *  the attribute that holds the working directory
   */
  lazy val workingDir = preferredWorkingDir.getOrElse(Files.createTempDir().toPath)

  /**
   * set the working directory to be used.
   *
   * '''ensure that setWorkingDir is invoked before workingDir variable is accessed'''
   * if workingDir is accessed before setWorkingDir is set, then a random tmp directory is assigned and any further 
   * assignment of working directory via setWorkingDir will not have any effect. 
   * @param working_dir the working directory to be used
   */
  def setWorkingDir(working_dir: Path) = {
    preferredWorkingDir = Some(working_dir)
  }

  /**
    * creates a file associated with a task in the working directory of the job.
    * A new directory with the name of the task is created in the working directory if it doesn't already exists
    * The file is created inside this task directory.
    * @param fileName name of the file
    * @param taskName name of the task. If the taskName is not specified the current thread name is used.
    * @return File object of the newly created file.
    */
  def getTaskFile(fileName: String, taskName: Option[String] = None) = {
    val parent = new File(joinPath(workingDir.toString,
      taskName getOrElse Thread.currentThread().getName))
    parent.mkdirs()
    new File(parent, fileName)
  }


  /**
    * creates a directory associated with a task in the working directory of the job.
    *
    * @param dirName name of the directory to be created.
    * @param taskName optional taskname. If the taskName is not specified the current thread name is used.
    * @return
    */
  def getTaskDirectory(dirName: String, taskName: Option[String] = None) = {
    val dir = new File(joinPath(workingDir.toString,taskName getOrElse Thread.currentThread().getName, dirName))
    dir.mkdirs()
    dir
  }

  def getDefaults(component: String, task: String) = {
    payload.getAs[Config](s"${Keywords.Config.DEFAULTS}.$component.$task").getOrElse(ConfigFactory.empty)
  }


}



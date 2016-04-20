package org.ultron.task

import java.io.{File, PrintWriter}

import com.google.common.io.Files
import com.typesafe.config.Config
import org.ultron.core.LogSource
import org.ultron.util.FileSystemUtil

import scala.io.Source


/**
 * Created by chlr on 3/3/16.
 */


/**
 * a generic class that defined lifecycle methods of a task.
 *
 * This abstract class defines lifecycle methods like setup, work, teardown.
 * Additionally it provides common helper methods like writeToFile, getFileHandle and readFile.
 *
 * @constructor creates a new instance of Task.
 *
 * @param taskName name of the task
 */

abstract class Task(val taskName: String) {

  /**
   *  this implicit val helps [[org.ultron.core.AppLogger]] identify which specific instance of Task logged the given log message.
   */
  implicit protected var source: LogSource = null

  private[task] def setLogSource(source: LogSource) = {
    this.source = source
  }

  /**
   *
   * @param content the content to write in the file.
   * @param fileName the name of the file to write the content
   * @return the java.io.File handle
   */
  protected def writeToFile(content: String, fileName: String) = {
    val file = this.getFileHandle(fileName)
    Files.createParentDirs(file)
    val writer = new PrintWriter(file)
    writer.write(content)
    writer.close()
    file
  }

  /**
   * get File object for `fileName`
   *
   * returns the java.io.File object for a give file `fileName` located within the working directory of the task.
   *
   * @param fileName name of the file to accessed
   * @return an instance of java.io.File representing the requested file.
   */
  protected def getFileHandle(fileName: String) = {
    new File(FileSystemUtil.joinPath(TaskContext.workingDir.toString,taskName),fileName)
  }

  /**
   *
   * @param fileName name of the file to be read
   * @return String content of the file.
   */
  protected def readFile(fileName: String) = {
    Source.fromFile(getFileHandle(fileName)).getLines().mkString
  }

  /**
   * override this to implement the setup phase
   *
   * This method should have setup phase of the task, which may include actions like
   *  - creating database connections
   *  - generating relevant files
   */
  protected[task] def setup(): Unit

  /**
   * override this to implemet the work phase
   *
   * this is where the actual work of the task is done, such as
   *  - executing query
   *  - launching subprocess
   *
   * @return any output of the work phase be encoded as a HOCON Config object.
   */
  protected[task] def work(): Config


  /**
   * override this to implement the teardown phase
   *
   * this is where you deallocate any resource you have acquired in setup phase.
   */
  protected[task] def teardown(): Unit

  def execute: Config = {
    this.setup()
    val result = this.work()
    this.teardown()
    result
  }

}


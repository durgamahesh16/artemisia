package tech.artemisia.task

import java.nio.file.Path
import tech.artemisia.util.HoconConfigUtil
import HoconConfigUtil.Handler
import com.google.common.io.Files
import com.typesafe.config.{ConfigFactory, Config}
import tech.artemisia.task.settings.DBConnection$
import scala.collection.JavaConverters._


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
   * the entire payload. This field exists here to facilitate subsitution during very late stage
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




}



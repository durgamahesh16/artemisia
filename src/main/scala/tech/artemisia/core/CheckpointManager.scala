package tech.artemisia.core

import java.io.{PrintWriter, File}
import com.typesafe.config.{ConfigRenderOptions, Config, ConfigFactory, ConfigObject}
import tech.artemisia.core.dag.Message.TaskStats
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.collection.JavaConverters._

/**
 * Created by chlr on 5/23/16.
 */

/**
 * This class manages checkpointing mechanism.
 * It is responsible for writing checkpoints to file.
 * and parsing checkpoint file to bootstrap a Dag
 * @param checkpointFile checkpoint file
 */
class CheckPointManager(checkpointFile: File) {

   var (adhocPayload, taskStatRepo) =
    if (checkpointFile.exists())
    this.parseCheckPointFile
    else
      ConfigFactory.empty() -> Map[String, TaskStats]()


  /**
   * Saves the TaskStats of a task named taskName
   * This method is not threadsafe and hence must be invoked within the actor context
   * only by the dag co-ordinater node and not by worker nodes.
   * @param taskName name of the task
   * @param taskStat task's stats
   */
  def save(taskName: String, taskStat: TaskStats): Unit = {
    adhocPayload = taskStat.taskOutput withFallback adhocPayload
    taskStatRepo = taskStatRepo + (taskName -> taskStat)
    flush()
  }


  /**
   * parse a standard checkpoint file and construct the checkpoint data-structure
   * @return a tuple of adhoc payload and a map of taskname and task stats
   */
  private[core] def parseCheckPointFile = {
    val config = ConfigFactory parseFile checkpointFile
    val taskStats = config.as[Config](Keywords.Checkpoint.TASK_STATES)
    val taskStatMap = taskStats.root().asScala map {
      case (key: String, value: ConfigObject) => key -> TaskStats(value.toConfig)
      case _ => throw new RuntimeException("invalid checkpoint ")
    }
    config.as[Config](Keywords.Checkpoint.PAYLOAD) -> taskStatMap.toMap
  }


  /**
   * serialize the checkpoint data-structure to a standard checkpoint config object
   * @return
   */
  private[core] def serializeCheckPointConfig: Config = {
    var config = ConfigFactory.empty()
    val taskStateConfig = taskStatRepo.foldLeft(ConfigFactory.empty()) {
      case (transitiveConfig, ( taskName, taskStat: TaskStats)) => taskStat.toConfig(taskName) withFallback transitiveConfig
    }
    config = config.withValue(Keywords.Checkpoint.PAYLOAD, adhocPayload.root())
    config = config.withValue(Keywords.Checkpoint.TASK_STATES, taskStateConfig.root())
    config
  }

  /**
   * flush the task stats and adhoc payload to checkpoint file
   */
  private def flush() = {
    val pw = new PrintWriter(checkpointFile)
    pw.write(serializeCheckPointConfig.root().render(ConfigRenderOptions.concise()))
    pw.close()
  }

}


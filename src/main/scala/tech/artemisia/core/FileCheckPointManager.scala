package tech.artemisia.core

import java.io.{File, PrintWriter}

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import tech.artemisia.dag.Message.TaskStats
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.collection.JavaConverters._

/**
 * Created by chlr on 5/23/16.
 */

/**
 * This class manages check-pointing mechanism.
 * It is responsible for writing checkpoints to file.
 * and parsing checkpoint file to bootstrap a Dag
 * @todo adhoc-payload can be computed by merge all tasks output from TaskStats.
 *       currently we save both task output in each TaskStat instance and also the consolidate adhoc-payload
 *       in the checkpoint file separately in redundant fashion. This is because currently we don't know in what order each TaskStats node's
 *       task-output configs must be merged to get the right adhoc-payload config since this ordering information
 *       of TaskStats instance is not maintained in the checkpoint file.
 * @param checkpointFile checkpoint file
 */
class FileCheckPointManager(checkpointFile: File) extends BasicCheckpointManager {

     (if (checkpointFile.exists()) this.parseCheckPointFile else
                              ConfigFactory.empty() -> Map[String, TaskStats]()) match {
       case (x,y) => adhocPayload = x; taskStatRepo = y
     }

  /**
   * Saves the TaskStats of a task named taskName
   * This method is not threadsafe and hence must be invoked within the actor context
   * only by the dag co-ordinater node and not by worker nodes.
   * @param taskName name of the task
   * @param taskStat task's stats
   */
  override private[core] def save(taskName: String, taskStat: TaskStats): Unit = {
    super.save(taskName, taskStat)
    flush()
  }


  /**
   * parse a standard checkpoint file and construct the checkpoint data-structure
   * @return a tuple of adhoc payload and a map of taskname and task stats
   */
  private def parseCheckPointFile = {
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
  private def serializeCheckPointConfig: Config = {
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


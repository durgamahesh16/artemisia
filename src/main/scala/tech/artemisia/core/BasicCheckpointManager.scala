package tech.artemisia.core

import com.typesafe.config.{ConfigFactory, Config}
import tech.artemisia.dag.Message.TaskStats

/**
 * Created by chlr on 5/23/16.
 */

/**
  * Checkpoint Manager that persists checkpoint data in-memory.
  * This checkpoint manager doesn't parse existing checkpoints
  * and all new checkpoints are kept in memory.
  *
  */
class BasicCheckpointManager {

  protected var adhocPayload: Config = ConfigFactory.empty()
  protected var taskStatRepo: Map[String,TaskStats] = Map()

  private[core] def save(taskName: String, taskStat: TaskStats) = {
    adhocPayload = taskStat.taskOutput withFallback adhocPayload
    taskStatRepo = taskStatRepo + (taskName -> taskStat)
  }

  private[core] def checkpoints = {
    BasicCheckpointManager.CheckpointData(adhocPayload, taskStatRepo)
  }

}


object BasicCheckpointManager {

  case class CheckpointData(adhocPayload: Config = ConfigFactory.empty(), taskStatRepo: Map[String, TaskStats] = Map())

}

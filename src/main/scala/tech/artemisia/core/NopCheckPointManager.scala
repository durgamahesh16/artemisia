package tech.artemisia.core

import com.typesafe.config.ConfigFactory
import tech.artemisia.core.CheckpointManager.CheckpointData
import tech.artemisia.dag.Message.TaskStats

/**
 * Created by chlr on 5/23/16.
 */

/**
 * An empty implementation of CheckpointManager that does nothing.
 *
 */
object NopCheckPointManager extends CheckpointManager {

  /**
   * empty implementation of this task that does nothing
   * @param taskName name of the task
   * @param taskStats stats of the task
   */
  override def save(taskName: String, taskStats: TaskStats): Unit = {}

  /**
   *
   * @return a empty checkpoint data
   */
  override private[core] def checkpoints: CheckpointData = {
    CheckpointData(adhocPayload = ConfigFactory.empty(), taskStatRepo = Map())
  }

}

package tech.artemisia.core

import com.typesafe.config.Config
import tech.artemisia.dag.Message.TaskStats

/**
 * Created by chlr on 5/23/16.
 */
trait CheckpointManager {

  var adhocPayload: Config

  var taskStatRepo: Map[String, TaskStats]

  private[core] def save(taskName: String, taskStats: TaskStats)

}

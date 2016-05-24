package tech.artemisia.core

import com.typesafe.config.Config
import tech.artemisia.core.dag.Message.TaskStats

/**
 * Created by chlr on 5/23/16.
 */
trait CheckpointManager {

  var adhocPayload: Config

  var taskStatRepo: Map[String, TaskStats]

  def save(taskName: String, taskStats: TaskStats)

}

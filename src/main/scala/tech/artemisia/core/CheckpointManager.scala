package tech.artemisia.core

import com.typesafe.config.{ConfigFactory, Config}
import tech.artemisia.dag.Message.TaskStats

/**
 * Created by chlr on 5/23/16.
 */
trait CheckpointManager {

  private[core] def save(taskName: String, taskStats: TaskStats)

  private[core] def checkpoints: CheckpointManager.CheckpointData

}


object CheckpointManager {

  case class CheckpointData(adhocPayload: Config = ConfigFactory.empty(), taskStatRepo: Map[String, TaskStats] = Map())

}

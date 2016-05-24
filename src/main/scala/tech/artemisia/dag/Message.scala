package tech.artemisia.dag

import com.typesafe.config.{ConfigRenderOptions, Config, ConfigFactory}
import tech.artemisia.core.Keywords
import tech.artemisia.util.HoconConfigUtil
import HoconConfigUtil.Handler
import tech.artemisia.task.TaskHandler

/**
 * Created by chlr on 1/7/16.
 */
object Message {

  sealed trait Messageable

  class Tick extends Messageable

  class TaskCompleted extends Messageable

  case class TaskFailed(name: String, taskStats: TaskStats, exception: Throwable)
    extends TaskCompleted

  case class TaskWrapper(name: String,task: TaskHandler) extends Messageable

  case class TaskSuceeded(name: String, task_stats: TaskStats) extends TaskCompleted


  case class TaskStats  (
                        startTime: String,
                        endTime: String = null,
                        status: Status.Value,
                        attempts: Int = 1,
                        taskOutput: Config = ConfigFactory.empty()
                        ) extends Messageable {

    def toConfig(task_name: String) = {

      ConfigFactory parseString {
        s"""
          |$task_name = {
          |  ${Keywords.TaskStats.START_TIME} = "$startTime"
          |  ${Keywords.TaskStats.END_TIME} = "$endTime"
          |  ${Keywords.TaskStats.STATUS} = $status
          |  ${Keywords.TaskStats.ATTEMPT} = $attempts
          |  ${Keywords.TaskStats.TASK_OUTPUT} = ${taskOutput.root().render(ConfigRenderOptions.concise())}
          |}
        """.stripMargin
      }
    }
  }


  object TaskStats {

     def apply(content: Config): TaskStats = {

       TaskStats(content.as[String](Keywords.TaskStats.START_TIME),
                content.as[String](Keywords.TaskStats.END_TIME),
                Status.withName(content.as[String](Keywords.TaskStats.STATUS)),
                content.as[Int](Keywords.TaskStats.ATTEMPT),
                content.as[Config](Keywords.TaskStats.TASK_OUTPUT))
    }
  }

}

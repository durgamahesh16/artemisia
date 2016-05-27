package tech.artemisia.task

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.{AppLogger, LogSource}
import tech.artemisia.dag.Status
import tech.artemisia.util.Util

import scala.util.{Failure, Success, Try}


/**
 * Created by chlr on 1/7/16.
 */

class TaskHandler(val taskConfig: TaskConfig, val task: Task) {

  task.setLogSource(LogSource(taskConfig.taskName))
  private var attempts = 0
  private var status: Status.Value = Status.UNKNOWN

  final def execute(): Try[Config] = {

    if(taskConfig.conditions._1) {
      AppLogger info s"running task with total allowed attempts of ${taskConfig.retryLimit}"
      val result = run(maxAttempts = taskConfig.retryLimit) {
          AppLogger debug "executing setup phase of the task"
          task.setup()
          AppLogger debug "executing work phase of the task"
          task.work()
      }
      try {   // teardown must run even if the task setup or work has failed
        AppLogger debug "executing teardown phase of the task"
        task.teardown()
      } catch {
        case ex: Throwable => AppLogger warn s"teardown phase failed with exception ${ex.getClass.getCanonicalName} with message ${ex.getMessage}"
      }
      result
    } else {
      AppLogger info s"skipping execution of ${taskConfig.taskName} since  ${taskConfig.conditions._2} failed"
      status = Status.SKIPPED
      Success(ConfigFactory.empty())
    }
  }

  private def run(maxAttempts: Int)(body : => Config) : Try[Config] = {
    try {
      attempts += 1
      val result = body
      this.status = Status.SUCCEEDED
      Success(result)
    } catch {
      case ex: Throwable => {
        AppLogger info s"attempt ${taskConfig.retryLimit - maxAttempts + 1} for task ${taskConfig.taskName}"
        AppLogger error Util.printStackTrace(ex)
        if (maxAttempts > 1) {
          run(maxAttempts -1)(body)
        }
        else {
          status = Status.FAILED
          Failure(ex)
        }
      }
    }
  }

  def getAttempts = attempts

  def getStatus = status

}


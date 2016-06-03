package tech.artemisia.task

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.{AppLogger, BooleanEvaluator, Keywords}
import tech.artemisia.dag.Status
import tech.artemisia.util.HoconConfigUtil.configToConfigEnhancer
import tech.artemisia.util.Util

import scala.util.{Failure, Success, Try}

/**
  * Created by chlr on 1/7/16.
  */

class TaskHandler(val taskConfig: TaskConfig, val task: Task) {

  private var attempts = 0
  private var status: Status.Value = Status.UNKNOWN

  final def execute(): Try[Config] = {

    taskConfig.conditions match {
      case Some((true, x)) => {
        AppLogger info s"executing task ${task.taskName} since expression $x succeeded"
        executeLifeCycles
      }
      case None => executeLifeCycles
      case Some((false, x)) => {
        AppLogger info s"skipping execution of ${task.taskName} since expression $x failed"
        status = Status.SKIPPED
        Success(ConfigFactory.empty())
      }
    }
  }

  def getAttempts = attempts

  def getStatus = status


  private def executeLifeCycles = {
    val result = lifecyles()
    result match {
      case Success(resultConfig) => runAssertions(resultConfig)
      case _ => ()
    }
    result
  }

  private def runAssertions(taskResult: Config) = {
    val effectiveConfig = taskResult withFallback TaskContext.payload
    val assertionKey = s"${task.taskName}.${Keywords.Task.ASSERTION}"
    taskConfig.assertion match {
      case Some((configValue, desc)) => {
        AppLogger debug s"running assertions on ${task.taskName}"
        val resolvedConfig: Config = effectiveConfig.withValue(assertionKey, configValue).hardResolve
        val result = BooleanEvaluator.evalBooleanExpr(resolvedConfig.getValue(assertionKey))
        assert(result, desc)
      }
      case None => ()
    }
  }

  private def lifecyles(): Try[Config] = {

    AppLogger info s"running task with total allowed attempts of ${taskConfig.retryLimit}"

    val result = run(maxAttempts = taskConfig.retryLimit) {
      AppLogger debug "executing setup phase of the task"
      task.setup()
      AppLogger debug "executing work phase of the task"
      task.work()
    }

    try {
      // teardown must run even if the task setup or work has failed
      AppLogger debug "executing teardown phase of the task"
      task.teardown()
    } catch {
      case ex: Throwable => AppLogger warn s"teardown phase failed with exception ${ex.getClass.getCanonicalName} with message ${ex.getMessage}"
    }
    result
  }

  private def run(maxAttempts: Int)(body: => Config): Try[Config] = {
    try {
      attempts += 1
      val result = body
      this.status = Status.SUCCEEDED
      Success(result)
    } catch {
      case ex: Throwable => {
        AppLogger info s"attempt ${taskConfig.retryLimit - maxAttempts + 1} for task ${task.taskName}"
        AppLogger error Util.printStackTrace(ex)
        if (maxAttempts > 1) {
          run(maxAttempts - 1)(body)
        }
        else {
          status = Status.FAILED
          Failure(ex)
        }
      }
    }
  }

}


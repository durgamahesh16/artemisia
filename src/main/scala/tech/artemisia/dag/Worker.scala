package tech.artemisia.dag

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import Message._
import tech.artemisia.core.AppLogger
import tech.artemisia.dag.Message.{TaskCompleted, TaskWrapper}
import tech.artemisia.util.Util

import scala.util.Try

/**
 * Created by chlr on 1/7/16.
 */
class Worker extends Actor {

  override def receive: Receive = {
    case message: TaskWrapper => {
      Thread.currentThread().setName(message.name)
      AppLogger info s"task ${message.name} has been submitted for execution"
      val start_time = Util.currentTime
      val result: Try[TaskCompleted] = message.task.execute() map {
        result: Config => {
          TaskSuceeded(message.name,TaskStats(start_time,Util.currentTime,message.task.getStatus,message.task.getAttempts,result))
        }
      } recover {
          case th: Throwable => {
            TaskFailed(message.name,TaskStats(start_time, Util.currentTime,
              if(message.task.taskConfig.ignoreFailure) Status.FAILURE_IGNORED else Status.FAILED,
            message.task.getAttempts, ConfigFactory.empty()), th)
          }
      }
      sender ! result.get
    }
  }

}

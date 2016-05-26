package tech.artemisia.task

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.util.HoconConfigUtil
import HoconConfigUtil.Handler
import tech.artemisia.core.{Utility, AppContext, Keywords}
import tech.artemisia.core.Keywords.Task

import scala.concurrent.duration.FiniteDuration

/**
 * Created by chlr on 1/9/16.
 */


/*
TaskConfig requires task_name param because the generic Task node requires task_name variable which will be used in logging.
 */
case class TaskConfig(taskName: String, retryLimit : Int, cooldown: FiniteDuration, conditions: Boolean = true,
                   ignoreFailure: Boolean = false) {

}

object TaskConfig {

  def apply(taskName: String, inputConfig: Config, appContext: AppContext): TaskConfig = {

    val default_config = ConfigFactory parseString {
      s"""
         |${Task.IGNORE_ERROR} = ${appContext.dagSetting.ignore_conditions}
         |${Keywords.Task.ATTEMPT} = ${appContext.dagSetting.attempts}
         |${Keywords.Task.CONDITION} = true
         |${Keywords.Task.COOLDOWN} = ${appContext.dagSetting.cooldown}
         |__context__ = {}
    """.stripMargin
    }


    val config = inputConfig withFallback default_config
    TaskConfig(taskName,config.as[Int](Keywords.Task.ATTEMPT),
      config.as[FiniteDuration](Keywords.Task.COOLDOWN),
      Utility.evalBooleanExpr(config.getAnyRef(Keywords.Task.CONDITION)),
      config.as[Boolean](Keywords.Task.IGNORE_ERROR))
  }

}

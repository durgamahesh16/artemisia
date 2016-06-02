package tech.artemisia.task

import com.typesafe.config._
import tech.artemisia.core.Keywords.Task
import tech.artemisia.core.{AppContext, BooleanEvaluator, Keywords}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * Created by chlr on 1/9/16.
 */


/*
TaskConfig requires task_name param because the generic Task node requires task_name variable which will be used in logging.
 */
case class TaskConfig(taskName: String, retryLimit : Int, cooldown: FiniteDuration, conditions: Option[(Boolean, String)] = None,
                   ignoreFailure: Boolean = false, assertion: Option[(ConfigValue, String)] = None)

object TaskConfig {

  def apply(taskName: String, inputConfig: Config, appContext: AppContext): TaskConfig = {

    val default_config = ConfigFactory parseString {
      s"""
         |${Task.IGNORE_ERROR} = ${appContext.dagSetting.ignore_conditions}
         |${Keywords.Task.ATTEMPT} = ${appContext.dagSetting.attempts}
         |${Keywords.Task.COOLDOWN} = ${appContext.dagSetting.cooldown}
         |__context__ = {}
    """.stripMargin
    }

    val config = inputConfig withFallback default_config
      TaskConfig(taskName,config.as[Int](Keywords.Task.ATTEMPT),
      config.as[FiniteDuration](Keywords.Task.COOLDOWN),
      config.getAs[ConfigValue](Keywords.Task.CONDITION) map { parseConditionsNode } map { case (x,y) => BooleanEvaluator.evalBooleanExpr(x) -> y },
      config.as[Boolean](Keywords.Task.IGNORE_ERROR),
      config.getAs[ConfigValue](Keywords.Task.ASSERTION) map { parseConditionsNode }
      )
  }


  private[task] def parseConditionsNode(input: ConfigValue): (ConfigValue,String) = {
    input match {
      case x: ConfigObject => {
        x.keySet().asScala.toList.sorted match {
          case Seq(BooleanEvaluator.description, BooleanEvaluator.expression) =>
            x.toConfig.as[ConfigValue](BooleanEvaluator.expression) -> x.toConfig.as[String](BooleanEvaluator.description)
          case _ => x -> BooleanEvaluator.stringifyBoolExpr(x)
        }
      }
      case _ =>  input -> BooleanEvaluator.stringifyBoolExpr(input)
    }
  }
  
  
}

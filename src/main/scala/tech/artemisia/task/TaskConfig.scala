package tech.artemisia.task

import com.typesafe.config.{ConfigValue, Config, ConfigFactory}
import tech.artemisia.util.HoconConfigUtil
import HoconConfigUtil.Handler
import tech.artemisia.core.{BooleanEvaluator, AppContext, Keywords}
import tech.artemisia.core.Keywords.Task
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * Created by chlr on 1/9/16.
 */


/*
TaskConfig requires task_name param because the generic Task node requires task_name variable which will be used in logging.
 */
case class TaskConfig(taskName: String, retryLimit : Int, cooldown: FiniteDuration, conditions: (Boolean, String) = true -> "",
                   ignoreFailure: Boolean = false) {

}

object TaskConfig {

  def apply(taskName: String, inputConfig: Config, appContext: AppContext): TaskConfig = {

    val default_config = ConfigFactory parseString {
      s"""
         |${Task.IGNORE_ERROR} = ${appContext.dagSetting.ignore_conditions}
         |${Keywords.Task.ATTEMPT} = ${appContext.dagSetting.attempts}
         |${Keywords.Task.CONDITION} = yes
         |${Keywords.Task.COOLDOWN} = ${appContext.dagSetting.cooldown}
         |__context__ = {}
    """.stripMargin
    }

    val config = inputConfig withFallback default_config
    TaskConfig(taskName,config.as[Int](Keywords.Task.ATTEMPT),
      config.as[FiniteDuration](Keywords.Task.COOLDOWN),
      parseConditionsNode(config.getValue(Keywords.Task.CONDITION)),
      config.as[Boolean](Keywords.Task.IGNORE_ERROR))
  }


  private[task] def parseConditionsNode(input: ConfigValue) = {
    input.unwrapped() match {
      case x: java.util.Map[String, Any] @unchecked => {
        x.asScala.keys.toList.sorted match {
          case Seq(BooleanEvaluator.description, BooleanEvaluator.expression) =>
            BooleanEvaluator.evalBooleanExpr(x.asScala(BooleanEvaluator.expression)) -> x.asScala(BooleanEvaluator.description).asInstanceOf[String]
          case _ => BooleanEvaluator.evalBooleanExpr(x) -> BooleanEvaluator.stringifyBoolExpr(x)
        }
      }
      case node @ _ =>  BooleanEvaluator.evalBooleanExpr(node) -> BooleanEvaluator.stringifyBoolExpr(node)
    }
  }
  
  
}

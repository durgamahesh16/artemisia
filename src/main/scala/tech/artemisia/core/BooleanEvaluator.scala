package tech.artemisia.core

/**
  * Created by chlr on 5/25/16.
  */

import java.util
import com.typesafe.config.{ConfigFactory, ConfigValue}
import tech.artemisia.util.HoconConfigUtil.Handler
import java.util.{ArrayList => JavaArrayList}
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.util.{Failure, Success, Try}


object BooleanEvaluator {

  val description = "description"
  val expression = "expression"

  def eval[A](expr: String): A = {
    val toolbox = universe.runtimeMirror(this.getClass.getClassLoader).mkToolBox()
    AppLogger debug s"executing expression $expr"
    toolbox.eval(toolbox.parse(expr)).asInstanceOf[A]
  }


  def evalBooleanExpr(exprTree: Any): Boolean = {

    def parser(tree: Any) = {
      tree match {
        case x: Boolean => x
        case x: String => Try((ConfigFactory parseString s" foo = $x ").as[Boolean]("foo")) match {
          case Success(bool) => bool
          case Failure(_) => eval[Boolean](x)
        }
        case x: util.List[String @unchecked] => {
          x.asScala map { eval[Boolean] } forall { in => in }
        }
        case x: util.Map[String, JavaArrayList[Any]] @unchecked => {
          x.asScala map {
            case ("or", value) => value.asScala map { evalBooleanExpr } exists { in => in }
            case ("and", value) => evalBooleanExpr(value)
          } forall { in => in }
        }
      }
    }

    exprTree match {
      case in: ConfigValue => parser(in.unwrapped())
      case in => parser(in)
    }

  }


  def stringifyBoolExpr(exprTree: Any): String = {

     def parser(tree: Any) = {
      tree match {
        case x: Boolean => x.toString
        case x: String => Try((ConfigFactory parseString s" foo = $x ").as[Boolean]("foo")) match {
          case Success(bool) => bool.toString
          case Failure(_) => x
        }
        case x: util.List[String @unchecked] => x.asScala map { stringifyBoolExpr } map { in => s"($in)" } mkString " and "
        case x: util.Map[String, JavaArrayList[Any]] @unchecked => {
          x.asScala map {
            case ("or", value) => value.asScala map { stringifyBoolExpr } map { in => s"($in)" } mkString " or "
            case ("and", value) => stringifyBoolExpr(value)
          } map (in => s"($in)") mkString " and "
        }
      }
    }

    exprTree match {
      case in: ConfigValue => parser(in.unwrapped())
      case in => parser(in)
    }
  }

}

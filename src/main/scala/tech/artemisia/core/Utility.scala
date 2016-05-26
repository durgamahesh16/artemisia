package tech.artemisia.core

/**
 * Created by chlr on 5/25/16.
 */

import java.util
import com.typesafe.config.ConfigFactory
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.util.{Failure, Success, Try}


object Utility {

  def eval[A](expr: String): A = {
    val toolbox = universe.runtimeMirror(this.getClass.getClassLoader).mkToolBox()
    toolbox.eval(toolbox.parse(expr)).asInstanceOf[A]
  }


  def evalBooleanExpr(tree: Any): Boolean = {
    tree match {
      case x: String => Try((ConfigFactory parseString s" foo = $x ").as[Boolean]("foo")) match {
        case Success(bool) => bool
        case Failure(_) => eval[Boolean](x)
      }
      case x: util.ArrayList[String @unchecked] =>  {
         x.asScala map { eval[Boolean] } forall { in => in }
      }
      case x: util.Map[String @unchecked,util.ArrayList[Any]] => {
        x.asScala.toIterable map {
          case ("or", value) =>  value.asScala map { evalBooleanExpr } exists { in => in }
          case ("and", value) => value.asScala map { evalBooleanExpr } forall { in => in }
        } forall { in => in }
      }
    }

  }

}

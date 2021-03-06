package tech.artemisia.util

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import tech.artemisia.task.TaskContext

import scala.collection.JavaConverters._
import scala.util.matching.Regex.Match

/**
 * Created by chlr on 4/25/16.
 */

/**
 * A pimp my Libary class for Hocon Config object that resolves quoted strings by doing DFS on the config tree
 * @param root resolved Config object
 */
class HoconConfigEnhancer(val root: Config)  {

  private var reference: Config = _

  def hardResolve(reference: Config = root) = {
    this.reference = reference
    resolveConfig(root.resolveWith(reference))
  }

  private def resolveConfig(config: Config): Config = {

    val processed = for (key <- config.root().keySet().asScala) yield {
      /*
          the keys are double quoted to handle Quoted ConfigString with dot as part of the key value.
           for eg. { "foo.bar" = baz } must not be parsed as { foo = { bar = baz} } since foo.bar is a single
           quoted key
       */
      config.getAnyRef(s""""$key"""")  match {
        case x: String => key -> ConfigValueFactory.fromAnyRef(HoconConfigEnhancer.resolveString(x, this.reference))
        case x: java.lang.Iterable[AnyRef] @unchecked => key -> ConfigValueFactory.fromAnyRef(resolveList(x.asScala))
        case x: java.util.Map[String, AnyRef] @unchecked => key -> ConfigValueFactory.fromMap(resolveConfig(config.getConfig(key)).root().unwrapped())
        case x => key -> ConfigValueFactory.fromAnyRef(x)
      }
    }
    processed.foldLeft(ConfigFactory.empty())({ (configValue, elems)  => configValue.withValue(s""""${elems._1}"""", elems._2) } )
  }


  private def resolveList(list: Iterable[Any]): java.lang.Iterable[Any] = {
    val processed: Iterable[Any] = for (node <- list) yield {
      node match {
        case x: java.util.Map[String,AnyRef] @unchecked => {
          resolveConfig(ConfigValueFactory.fromMap(x).toConfig).root().unwrapped()
        }
        case x: java.lang.Iterable[AnyRef] @unchecked => resolveList(x.asScala)
        case x: String => HoconConfigEnhancer.resolveString(x, this.reference)
        case x => x
      }
    }
    processed.asJava
  }

}


object HoconConfigEnhancer {

  private def resolveString(str: String, reference: Config) = {
    def replace(x: Match): String = {
      val variable = x.group(2)
      if (x.group(1) == "?")
        if (reference.hasPath(variable)) reference.getString(variable) else ""
      else
        reference.getString(variable)
    }
    val rgx = """\$\{(\??)(.+?)\}""".r
    rgx.replaceAllIn(str, replace _)
  }

  def stripLeadingWhitespaces(str: String): String = {
    val minWhiteSpace = str.split("\n") filter {
      _.trim.length > 0
    } map { """^[\s]+""".r.findFirstIn(_).getOrElse("").length} match {
      case Array() => 0
      case x => x.min
    }
    val result = str.split("\n") filter { _.trim.length > 0 } map { ("""^[\s]{"""+minWhiteSpace+"""}""").r.replaceFirstIn(_,"") }
    result.mkString("\n")
  }

  def readFileContent(file: File, reference: Config = TaskContext.payload) = {
    val content = scala.io.Source.fromFile(file).mkString
    resolveString(content,reference)
  }

}

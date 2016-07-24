package tech.artemisia.task

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.Keywords
import tech.artemisia.util.DocStringProcessor.StringUtil
import tech.artemisia.util.HoconConfigUtil

/**
  * Created by chlr on 6/7/16.
  */
trait TaskLike {

  /**
    * name of the task
    */
  val taskName: String

  /**
    *
    */
  val defaultConfig: Config

  /**
    * one line info about the task
    */
  val info: String

  /**
    * task info in brief
    */
  val desc: String

  /**
    * Sequence of config keys and their associated values
    */
  val paramConfigDoc: Config

  /**
    *
    * @param component name of the component
    * @return config structure of the task
    */
  final def configStructure(component: String): String = {
   val config = ConfigFactory parseString  s"""
       | {
       |   ${Keywords.Task.COMPONENT} = $component
       |   ${Keywords.Task.TASK} = $taskName
       | }
     """.stripMargin
    HoconConfigUtil.render(config.withValue("param", paramConfigDoc.root()).root())
  }


  /**
    * definition of the fields in task param config
    */
  val fieldDefinition: Map[String, AnyRef]


  /**
    * returns the brief documentation of the task
    *
    * @param component name of the component
    * @return task documentation
    */
  def doc(component: String) = {
    s"""
       |### $taskName:
       |
       |
       |#### Description:
       |
       | $desc
       |
       |#### Configuration Structure:
       |
       |
       |      ${configStructure(component).ident(5)}
       |
       |
       |#### Field Description:
       |
       | ${TaskLike.displayFieldListing(fieldDefinition) ident 1}
       |
     """.stripMargin

  }

  /**
    * config based constructor for task
    * @param name a name for the task
    * @param config param config node
    */
  def apply(name: String, config: Config): Task


}

object TaskLike {


  def displayFieldListing(fieldDefinition: Map[String, AnyRef], ident: Int = 0): String  = {
    fieldDefinition map {
      case (field, value: String) => s"${" " * ident}* $field: $value"
      case (field, value:(String, Seq[String]) @unchecked) =>
        s"""${" " * ident}* $field: ${value._1}
           |${value._2 map {x => s"${" " * (ident + 4)}* $x"} mkString "\n" }""".stripMargin
      case (field, value: Map[String, AnyRef] @unchecked) => s"${" " * ident}* $field:\n${displayFieldListing(value, ident+3)}"
    } mkString System.lineSeparator()
  }

}

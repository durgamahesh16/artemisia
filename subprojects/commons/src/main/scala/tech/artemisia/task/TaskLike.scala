package tech.artemisia.task

import com.typesafe.config.Config
import tech.artemisia.util.DocStringProcessor.StringUtil

/**
  * Created by chlr on 6/7/16.
  */
trait TaskLike {

  /**
    * name of the task
    */
  val taskName: String

  /**
    * one line info about the task
    */
  val info: String

  /**
    * task info in brief
    */
  val desc: String

  /**
    *
    * @param component name of the component
    * @return config structure of the task
    */
  def configStructure(component: String): String


  /**
    * definition of the fields in task param config
    */
  val fieldDefinition: Seq[(String, AnyRef)]


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


  def displayFieldListing(fieldDefinition: Seq[(String, AnyRef)], ident: Int = 0): String  = {
    fieldDefinition map {
      case (field, value: String) => s"${" " * ident}* $field: $value"
      case (field, value: Seq[(String, AnyRef)] @unchecked) => s"${" " * ident}* $field:\n${displayFieldListing(value, ident+3)}"
    } mkString System.lineSeparator()
  }

}

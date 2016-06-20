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
  val fieldDefinition: Seq[String]


  /**
    * returns the brief documentation of the task
    *
    * @param component name of the component
    * @return task documentation
    */
  def doc(component: String) = {
    s"""
       | $taskName
       | ${"=" * taskName.length}
       |
       | #### Description:
       |
       | $desc
       |
       | #### Configuration Structure
       |
       | ```
       |      ${configStructure(component).ident(8)}
       | ```
       |
       | #### Field Description:
       |
       |     ${fieldDefinition map { x => s"* $x" } mkString System.lineSeparator ident 5}
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

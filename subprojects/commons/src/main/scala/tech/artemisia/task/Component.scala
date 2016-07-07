package tech.artemisia.task

import com.typesafe.config.Config
import tech.artemisia.inventory.exceptions.UnknownTaskException
import tech.artemisia.util.Util
/**
 * Created by chlr on 3/3/16.
 */

/**
  * Component usually represents a Data system such as Database, Spark Cluster or Localhost
  * @param name name of the component
  */
abstract class Component(val name: String) {


  /**
    * list of supported task name
    */
  val tasks: Seq[TaskLike]


  /**
    * default config applicable to all task
    */
  val defaultConfig: Config

  /**
   * returns an instance of [[Task]] configured via the config object
   *
   * {{{
   *   dispatch("ScriptTask","mySampleScriptTask",config)
   * }}}
   *
   *
   * @param task task the Component has to execute
   * @param name name assigned to the instance of the task
   * @param config HOCON config payload with configuration data for the task
   * @return an instance of [[Task]]
   */
  def dispatchTask(task: String, name: String, config: Config): Task = {
    tasks filter { _.taskName == task } match {
      case x :: Nil => x.apply(name, config withFallback defaultConfig)
      case Nil => throw new UnknownTaskException(s"unknown task $task in component $name")
      case _ => throw new RuntimeException(s"multiple tasks named $task is register component $name")
    }
  }


  /**
    * one line description of the Component
    */
  val info: String

  /**
    * A brief overview of the components and the tasks it supports.
    */
  def doc = {

    val taskTable: Seq[Array[String]] =  Array("Task", "Description") +: tasks.map(x => Array(x.taskName, x.info))

    s"""/
        /$name
        /${"=" * name.length}
        /
        /$info
        /
        /${Util.prettyPrintAsciiTable(taskTable.toArray)}
        /
     """.stripMargin('/')
  }


  private def componentTaskTableDim = {
    val minimumTaskWidth = 13
    val minimumInfoWidth = 25
    (tasks.map(_.taskName.length).max + 5).max(minimumTaskWidth) -> (tasks.map(_.info.length).max + 5).max(minimumInfoWidth)
  }


  /**
    * get documentation of the task
    * @param task name of the task
    */
  def taskDoc(task: String): String = {
    tasks filter { _.taskName == task } match {
      case x :: Nil => x.doc(name)
      case Nil => throw new UnknownTaskException(s"unknown task $task in component $name")
      case _ => throw new RuntimeException(s"multiple tasks named $task is register component $name")
    }
  }

}

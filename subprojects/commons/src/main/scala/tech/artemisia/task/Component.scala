package tech.artemisia.task

import com.typesafe.config.Config

/**
 * Created by chlr on 3/3/16.
 */

/**
  * Component usually represents a Data system such as Database, Spark Cluster or Localhost
  * @param name name of the component
  */
abstract class Component(name: String) {


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
  def dispatchTask(task: String, name: String, config: Config): Task


  /**
    * A brief overview of the components and the tasks it supports.
    */
  val doc: String


  /**
    * get documentation of the task
    * @param task name of the task
    */
  def taskDoc(task: String): String

}

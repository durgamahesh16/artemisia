package tech.artemisia.task

import com.typesafe.config.Config

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
    * returns the brief documentation of the task
    *
    * @param component name of the component
    * @return task documentation
    */
  def doc(component: String): String

  /**
    * config based constructor for task
    * @param name a name for the task
    * @param config param config node
    */
  def apply(name: String, config: Config): Task

}

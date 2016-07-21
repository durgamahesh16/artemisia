package tech.artemisia.task

import com.typesafe.config.Config

/**
  * Created by chlr on 7/19/16.
  */

/**
  * Any Configuration nodes like export/load setting or connection definition must implement this trait
  * to define defaults, configuration structure and fieldDescriptions.
  */
trait ConfigurationNode {

  val defaultConfig: Config

  val structure: Config

  val fieldDescription: Map[String, Any]


}

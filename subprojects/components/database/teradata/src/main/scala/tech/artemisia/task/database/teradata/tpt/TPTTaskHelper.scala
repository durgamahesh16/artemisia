package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import tech.artemisia.task.TaskLike
import tech.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 9/12/16.
 */


trait TPTTaskHelper extends TaskLike {

  override def paramConfigDoc =  ConfigFactory.empty()
    .withValue("load",TPTLoadSetting.structure.root())
    .withValue("destination-table",ConfigValueFactory.fromAnyRef("target_table"))
    .withValue(""""dsn_[1]"""",ConfigValueFactory.fromAnyRef("my_conn @info(dsn name defined in connection node)"))
    .withValue(""""dsn_[2]"""",DBConnection.structure(1025).root())

  override def defaultConfig: Config = ConfigFactory.empty()
    .withValue("load", TPTLoadSetting.defaultConfig.root())


  override def fieldDefinition = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "destination-table" -> "destination table to load",
    "location" -> "path pointing to the source file",
    "load" -> TPTLoadSetting.fieldDescription
  )

}

object TPTTaskHelper {

  def supportedModes: Seq[String] = Seq("fastload", "default", "auto")

}
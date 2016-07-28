package tech.artemisia.task.database

import java.io.File
import java.net.URI
import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */

trait LoadTaskHelper extends TaskLike {

  /**
    * task name
    */
  val taskName = "SQLLoad"

  /**
    * brief description of the task
    */
  val info = "load a file into a table"

  /**
    * brief description of task
    */
  val desc: String =
    s"""
       |$taskName task is used to load content into a table typically from a file.
       |the configuration object for this task is as shown below.
    """.stripMargin

  /**
    * undefined default port
    */
  def defaultPort: Int

  def supportedModes: Seq[String]

  override def fieldDefinition = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "destination-table" -> "destination table to load",
    "location" -> "path pointing to the source file",
    s"load" -> BasicLoadSetting.fieldDescription
  )

  override def defaultConfig: Config = ConfigFactory.empty()
                                  .withValue("load", BasicLoadSetting.defaultConfig.root())

  override def apply(name: String, config: Config): Task = ???

  override def paramConfigDoc: Config = {
    val config = ConfigFactory parseString
      s"""
         | "dsn_[1]" = connection-name
         |  destination-table = "dummy_table @required"
         |  location = /var/tmp/file.txt
     """.stripMargin
    config
      .withValue("load",BasicLoadSetting.structure.root())
      .withValue(""""dsn_[2]"""",DBConnection.structure(defaultPort).root())
  }
}

object LoadTaskHelper {

  /**
    * factory method to build task objects that are sub-types of LoadTaskHelper
    *
    * @param name name of the task
    * @param config configuration node with task settings.
    * @tparam T concrete type of LoadTaskHelper to be constructed
    * @return instance of type T.
    */
  def create[T <: LoadFromFile : ClassTag](name: String, config: Config): LoadFromFile = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = BasicLoadSetting(config.as[Config]("load"))
    val location = new File(config.as[String]("load-path")).toURI
    implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[LoadFromFile]).getConstructor(classOf[String],
      classOf[String], classOf[URI], classOf[DBConnection], classOf[BasicLoadSetting]).newInstance(name, destinationTable,
      location ,connectionProfile, loadSettings)
  }

}

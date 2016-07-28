package tech.artemisia.task.hadoop

import com.typesafe.config.Config
import tech.artemisia.task.database.{BasicLoadSetting, LoadTaskHelper}
import tech.artemisia.task.settings.{DBConnection, LoadSetting}
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */


trait LoadFromHDFSHelper extends LoadTaskHelper {

  override val taskName: String = "LoadFromHDFSHelper"

  def defaultPort: Int

  override def paramConfigDoc = super.paramConfigDoc.withValue("hdfs",HDFSReadSetting.structure.root())

  override def defaultConfig =  super.defaultConfig
                     .withValue("hdfs", HDFSReadSetting.defaultConfig.root())

  override def fieldDefinition: Map[String, AnyRef] = super.fieldDefinition - "location" + ("hdfs" -> HDFSReadSetting.fieldDescription)

  override val info: String = "Load Table from HDFS"

  override val desc: String = ""

}

object LoadFromHDFSHelper {

  def create[T <: LoadFromHDFS: ClassTag](name: String, config: Config): T = {
    val loadSettings = BasicLoadSetting(config.as[Config]("load"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val tableName = config.as[String]("destination-table")
    val hdfsReadSetting = HDFSReadSetting(config.as[Config]("hdfs"))
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[HDFSReadSetting], classOf[DBConnection],
      classOf[LoadSetting]).newInstance(name, tableName, hdfsReadSetting, connectionProfile, loadSettings).asInstanceOf[T]
  }

}

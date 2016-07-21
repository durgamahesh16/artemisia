package tech.artemisia.task.hadoop

import java.io.File
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database.LoadToTable
import tech.artemisia.task.settings.{BasicExportSetting, BasicLoadSetting, DBConnection, LoadSetting}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag

/**
  * Created by chlr on 7/19/16.
  */

abstract class LoadFromHDFS(override val taskName: String, override val tableName: String, val hdfsReadSetting: HDFSReadSetting,
                            override val  connectionProfile: DBConnection, override val loadSettings: LoadSetting)
  extends LoadToTable(taskName, tableName, hdfsReadSetting.location, connectionProfile ,loadSettings) {


  override val source = Left(HDFSUtil.mergeFileIOStreams(HDFSUtil.expandPath(hdfsReadSetting.location, filesOnly = false)
    , hdfsReadSetting.codec))

}

object LoadFromHDFS {

  val taskName: String = "LoadFromHDFS"

  def paramConfigDoc(port: Int) = LoadToTable.paramConfigDoc(port)
                        .withValue("hdfs",HDFSWriteSetting.structure.root())
                        .withoutPath("location")

  val defaultConfig: Config =  ConfigFactory.empty()
                        .withValue("load-setting", BasicLoadSetting.defaultConfig.root())
                        .withValue("hdfs", HDFSReadSetting.defaultConfig.root())

  val fieldDefinition: Map[String, AnyRef] = LoadToTable.fieldDefinition - "location" +
                                                  ("hdfs" -> HDFSReadSetting.fieldDescription)

  val info: String = "Load Table from HDFS"

  val desc: String = ""

  def create[T <: LoadFromHDFS: ClassTag](name: String, config: Config) = {
    val loadSettings = BasicLoadSetting(config.as[Config]("load-setting"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val tableName = new File(config.as[String]("destination-table")).toURI
    val hdfsReadSetting = HDFSReadSetting(config.as[Config]("hdfs"))
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[URI], classOf[DBConnection],
      classOf[BasicExportSetting]).newInstance(name, tableName, hdfsReadSetting, connectionProfile, loadSettings).asInstanceOf[LoadFromHDFS]
  }


}


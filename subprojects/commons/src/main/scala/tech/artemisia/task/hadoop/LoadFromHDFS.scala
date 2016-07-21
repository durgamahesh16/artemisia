package tech.artemisia.task.hadoop

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database.LoadToTable
import tech.artemisia.task.settings.{BasicLoadSetting, DBConnection, LoadSetting}

/**
  * Created by chlr on 7/19/16.
  */

abstract class LoadFromHDFS(override val taskName: String, override val tableName: String, val hdfsReadSetting: HDFSReadSetting,
                            override val  connectionProfile: DBConnection, override val loadSettings: LoadSetting)
  extends LoadToTable(taskName, tableName, hdfsReadSetting.location, connectionProfile ,loadSettings) {


  override val source = Left(HDFSUtil.readIOStream(hdfsReadSetting.location, hdfsReadSetting.codec))

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

}


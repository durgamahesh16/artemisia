package tech.artemisia.task.hadoop

import tech.artemisia.task.database.LoadFromFile
import tech.artemisia.task.settings.{DBConnection, LoadSetting}

/**
  * Created by chlr on 7/19/16.
  */

abstract class LoadFromHDFS(override val taskName: String, override val tableName: String, val hdfsReadSetting: HDFSReadSetting,
                            override val  connectionProfile: DBConnection, override val loadSetting: LoadSetting)
  extends LoadFromFile(taskName, tableName, hdfsReadSetting.location, connectionProfile ,loadSetting) {


  override lazy val source = Left(HDFSUtil.mergeFileIOStreams(HDFSUtil.expandPath(hdfsReadSetting.location)
    ,hdfsReadSetting.codec))

}

object LoadFromHDFS {

  def getPathForLoad(hdfsReadSetting: HDFSReadSetting) = {
      HDFSUtil.getPathForLoad(hdfsReadSetting.location, hdfsReadSetting.codec)
  }

}





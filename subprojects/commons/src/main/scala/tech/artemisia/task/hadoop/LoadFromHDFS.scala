package tech.artemisia.task.hadoop

import tech.artemisia.task.database.LoadFromFile
import tech.artemisia.task.settings.{DBConnection, LoadSetting}

/**
  * Created by chlr on 7/19/16.
  */

abstract class LoadFromHDFS(override val taskName: String, override val tableName: String, val hdfsReadSetting: HDFSReadSetting,
                            override val connectionProfile: DBConnection, override val loadSetting: LoadSetting)
  extends LoadFromFile(taskName, tableName, hdfsReadSetting.location, connectionProfile, loadSetting) {

  override lazy val source = Left(LoadFromHDFS.getInputStream(hdfsReadSetting))

}

object LoadFromHDFS {

  /**
    * get inputStream and size of the load path in bytes.
    * @param hdfsReadSetting hdfs read setting object
    * @return inputstream for the location in HDFSReadSetting
    */
  def getPathForLoad(hdfsReadSetting: HDFSReadSetting) = {
    hdfsReadSetting.cliMode match {
      case true =>
        val cliReader = new HDFSCLIReader(hdfsReadSetting.cliBinary)
        cliReader.readPath(hdfsReadSetting.location) -> cliReader.getPathSize(hdfsReadSetting.location)
      case false => HDFSUtil.getPathForLoad(hdfsReadSetting.location, hdfsReadSetting.codec)
    }
  }

  /**
    * get inputStream for the provided HDFSReadSetting object.
    * @param hdfsReadSetting hdfs read setting object
    * @return inputstream for the location in HDFSReadSetting
    */
  def getInputStream(hdfsReadSetting: HDFSReadSetting) = {
    hdfsReadSetting.cliMode match {
      case true => HDFSUtil.mergeFileIOStreams(HDFSUtil.expandPath(hdfsReadSetting.location)
        , hdfsReadSetting.codec)
      case false =>
        val reader = new HDFSCLIReader(hdfsReadSetting.getCLIBinaryPath)
        reader.readPath(hdfsReadSetting.location.toString)
    }
  }

}





package tech.artemisia.task.hadoop

import tech.artemisia.task.database.{DBInterface, ExportToFile}
import tech.artemisia.task.settings.{DBConnection, ExportSetting}


/**
  *
  * @param taskName name of the task
  * @param sql query for the export
  * @param connectionProfile Connection Profile settings
  * @param exportSetting Export settings
  * @param hdfsWriteSetting HDFS write settings
  */
abstract class ExportToHDFS(override val taskName: String, override val sql: String, val hdfsWriteSetting: HDFSWriteSetting,
                            override val connectionProfile: DBConnection, override val exportSetting: ExportSetting)
  extends ExportToFile(taskName, sql, hdfsWriteSetting.location ,connectionProfile , exportSetting) {

  val dbInterface: DBInterface

  override lazy val target = Left(HDFSUtil.writeIOStream(
    hdfsWriteSetting.location
    ,overwrite = hdfsWriteSetting.overwrite
    ,replication = hdfsWriteSetting.replication
    ,blockSize = hdfsWriteSetting.blockSize
    ,codec = hdfsWriteSetting.codec
  ))

}



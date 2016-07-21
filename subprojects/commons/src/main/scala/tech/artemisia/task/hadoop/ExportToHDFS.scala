package tech.artemisia.task.hadoop

import java.io.OutputStream

import com.typesafe.config.Config
import tech.artemisia.task.database.{DBInterface, ExportToFile}
import tech.artemisia.task.settings.{BasicExportSetting, DBConnection}


/**
  *
  * @param taskName name of the task
  * @param sql query for the export
  * @param connectionProfile Connection Profile settings
  * @param exportSettings Export settings
  * @param hdfsWriteSetting HDFS write settings
  */
abstract class ExportToHDFS(override val taskName: String,override val sql: String, hdfsWriteSetting: HDFSWriteSetting,
                            override val connectionProfile: DBConnection, override val exportSettings: BasicExportSetting)
  extends ExportToFile(taskName, sql, hdfsWriteSetting.location ,connectionProfile , exportSettings) {

  val dbInterface: DBInterface

  val outputStream: OutputStream = HDFSUtil.writeIOStream(
    hdfsWriteSetting.location
    ,overwrite = true
    ,bufferSize = 62914560
    ,replication = hdfsWriteSetting.replication
    ,blockSize = hdfsWriteSetting.blockSize
    ,codec = hdfsWriteSetting.codec
  )

}

object ExportToHDFS  {

  val taskName: String = "ExportToHDFS"

  def paramConfigDoc(port: Int) = ExportToFile.paramConfigDoc(port)
                        .withValue("hdfs", HDFSWriteSetting.structure.root())
                        .withoutPath("location")

  val defaultConfig: Config = ExportToFile.defaultConfig.withValue("hdfs", HDFSWriteSetting.defaultConfig.root())

  val fieldDefinition: Map[String, AnyRef] = ExportToFile.fieldDefinition - "location" +
                                                        ("hdfs" -> HDFSWriteSetting.fieldDescription)

  val info: String = "Export database resultset to HDFS"

  val desc: String = ""

}

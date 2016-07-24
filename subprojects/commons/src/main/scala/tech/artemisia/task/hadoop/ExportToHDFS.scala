package tech.artemisia.task.hadoop

import com.typesafe.config.Config
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.database.{BasicExportSetting, DBInterface, ExportToFile}
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag


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

object ExportToHDFS {

  val taskName: String = "ExportToHDFS"

  def paramConfigDoc(port: Int) = ExportToFile.paramConfigDoc(port)
                        .withValue("hdfs", HDFSWriteSetting.structure.root())
                        .withoutPath("location")

  val defaultConfig: Config = ExportToFile.defaultConfig.withValue("hdfs", HDFSWriteSetting.defaultConfig.root())

  val fieldDefinition: Map[String, AnyRef] = ExportToFile.fieldDefinition - "location" +
                                                        ("hdfs" -> HDFSWriteSetting.fieldDescription)

  val info: String = "Export database resultset to HDFS"

  val desc: String = ""

  def create[T <: ExportToHDFS: ClassTag](name: String, config: Config): T = {
    val exportSettings = BasicExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val hdfs = HDFSWriteSetting(config.as[Config]("hdfs"))
    val sql: String =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[HDFSWriteSetting],
      classOf[DBConnection], classOf[ExportSetting])
      .newInstance(name, sql, hdfs, connectionProfile, exportSettings).asInstanceOf[T]
  }

}

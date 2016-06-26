package tech.artemisia.task.database.teradata

import tech.artemisia.task.database.{DBInterface, DataTransporter}
import tech.artemisia.task.settings.{ExportSetting, LoadSettings}

/**
  * Created by chlr on 6/26/16.
  */
trait TDDataTransporter extends DataTransporter {

  self: DBInterface =>

  override def loadData(tableName: String, loadSettings: LoadSettings): (Long, Long) = ???

  override def exportData(sql: String, exportSetting: ExportSetting): Long = ???

}

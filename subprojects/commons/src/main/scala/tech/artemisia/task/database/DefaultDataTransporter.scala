package tech.artemisia.task.database

import tech.artemisia.task.settings.{ExportSetting, LoadSetting}

/**
 * Created by chlr on 5/1/16.
 */

/**
 * A mixin trait 
 */
trait DefaultDataTransporter extends  DataTransporter {

  self: DBInterface =>

  override def loadData(tableName: String, loadSetting: LoadSetting) = {
    assert(loadSetting.location.getScheme == "file", s"schema ${loadSetting.location.getScheme} is not supported. file:// is the only supported schema")
    val dbWriter = new DefaultDBWriter(tableName, loadSetting, this)
    this.batchInsert(dbWriter, loadSetting)
  }

  override def exportData(sql: String, exportSetting: ExportSetting) = {
    val rs = self.query(sql)
    DataTransporter.exportCursorToFile(rs, exportSetting)
  }

}










package tech.artemisia.task.database

import tech.artemisia.inventory.io.CSVFileReader
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}

/**
 * Created by chlr on 5/1/16.
 */

/**
 * A mixin trait 
 */
trait DefaultDataTransporter extends  DataTransporter {

  self: DBInterface =>

  override def loadData(tableName: String, loadSettings: LoadSetting) = {

    assert(loadSettings.location.getScheme == "file", s"schema ${loadSettings.location.getScheme} is not supported. file:// is the only supported schema")
    val dbWriter = new BatchDBWriter(tableName, loadSettings, this)
    val csvReader = new CSVFileReader(loadSettings)
    for (batch <- csvReader.grouped(loadSettings.batchSize)) {
        dbWriter.executeBatch(batch.toArray)
    }
    dbWriter.close()
    csvReader.rowCounter -> dbWriter.getErrRowCount
  }

  override def exportData(sql: String, exportSetting: ExportSetting) = {
    val rs = self.query(sql)
    DataTransporter.exportCursorToFile(rs, exportSetting)
  }

}










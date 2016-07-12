package tech.artemisia.task.database

import tech.artemisia.inventory.io.CSVFileReader
import tech.artemisia.task.settings.LoadSetting

/**
 * Created by chlr on 7/11/16.
 */


trait BatchDBImporter extends DBImporter {

  self: DBInterface =>

  def getBatchWriter(tableName: String, loadSetting: LoadSetting): BaseDBBatchWriter

  override final def load(tableName: String, loadSetting: LoadSetting): (Long, Long) = {
    val csvReader = new CSVFileReader(loadSetting)
    val dbWriter = getBatchWriter(tableName, loadSetting)
    for (batch <- csvReader.grouped(loadSetting.batchSize)) {
      dbWriter.processBatch(batch.toArray)
    }
    dbWriter.close()
    csvReader.rowCounter -> dbWriter.getErrRowCount
  }

}

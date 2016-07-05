package tech.artemisia.task.database

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.ResultSet

import com.opencsv.CSVWriter
import tech.artemisia.core.AppLogger
import tech.artemisia.inventory.io.CSVFileReader
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}

/**
  * Created by chlr on 6/12/16.
  */

/**
  *
  */
trait DataTransporter {

  self: DBInterface =>


  /**
    * A generic function that loads a file to table by iterating each row of the file
    * and running INSERT INTO TABLE query
    *
    * @param tableName target table to load
    * @param loadSettings load settings
    * @return number of records inserted
    */
  def loadData(tableName: String, loadSettings: LoadSetting): (Long, Long)


  /**
    * export query to file
    *
    * @param sql query
    * @param exportSetting export settings
    * @return no of records exported
    */
  def exportData(sql: String, exportSetting: ExportSetting): Long


  /**
    * Insert records using Database Writer using batches
    * @param dbWriter database writer object
    * @param loadSetting load settings
    * @return binary tuple of source rows read and rows rejected
    */
  protected def batchInsert(dbWriter: BaseDBWriter, loadSetting: LoadSetting) = {
    dbWriter.preLoad()
    val csvReader = new CSVFileReader(loadSetting)
    for (batch <- csvReader.grouped(loadSetting.batchSize)) {
      dbWriter.processBatch(batch.toArray)
    }
    dbWriter.postLoad()
    dbWriter.close()
    csvReader.rowCounter -> dbWriter.getErrRowCount
  }

  /**
    * Insert records using Database Writer row by row
    * @param dbWriter database writer object
    * @param loadSetting load settings
    * @return binary tuple of source rows read and rows rejected
    */
  protected def rowInsert(dbWriter: BaseDBWriter, loadSetting: LoadSetting) = {
    val csvReader = new CSVFileReader(loadSetting)
    for (row <- csvReader) {
      dbWriter.processRow(row)
    }
    dbWriter.postLoad()
    dbWriter.close()
    csvReader.rowCounter -> dbWriter.getErrRowCount
  }

}

object DataTransporter {


  /**
    *
    * @param resultSet ResultSet to be exported
    * @param exportSettings ExportSetting object
    * @todo emit total number of records exported
    * @return total no of rows exported
    */
  def exportCursorToFile(resultSet: ResultSet, exportSettings: ExportSetting): Long = {
    var recordCounter = 0L
    AppLogger info s"exporting result-set to file: ${exportSettings.file.getPath}"
    val buffer = new BufferedWriter(new FileWriter(new File(exportSettings.file)))
    val csvWriter = new CSVWriter(buffer, exportSettings.delimiter,
      if (exportSettings.quoting) exportSettings.quotechar else CSVWriter.NO_QUOTE_CHARACTER, exportSettings.escapechar)
    val data = DBUtil.streamResultSet(resultSet, header = exportSettings.header)
    for (record <- data) {
      recordCounter += 1
      csvWriter.writeNext(record)
    }
    recordCounter = if (exportSettings.header) recordCounter -1 else recordCounter // recordCounter counts header too
    buffer.close()
    AppLogger info s"exported $recordCounter rows to ${exportSettings.file.getPath}"
    recordCounter
  }
}

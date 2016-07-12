package tech.artemisia.task.database

import java.io.{File, FileWriter, BufferedWriter}

import com.opencsv.CSVWriter
import tech.artemisia.core.AppLogger
import tech.artemisia.task.settings.ExportSetting

/**
 * Created by chlr on 7/10/16.
 */
trait DefaultDBExporter extends DBDataExporter {

self: DBInterface =>

  override def export(sql: String, exportSetting: ExportSetting): Long = {
    val resultSet = self.query(sql)
    var recordCounter = 0L
    AppLogger info s"exporting result-set to file: ${exportSetting.file.getPath}"
    val buffer = new BufferedWriter(new FileWriter(new File(exportSetting.file)))
    val csvWriter = new CSVWriter(buffer, exportSetting.delimiter,
      if (exportSetting.quoting) exportSetting.quotechar else CSVWriter.NO_QUOTE_CHARACTER, exportSetting.escapechar)
    val columnCount = resultSet.getMetaData.getColumnCount
    if (exportSetting.header) {
      val header = for (i <- 1 to columnCount) yield { resultSet.getMetaData.getColumnLabel(i) }
      csvWriter.writeNext(header.toArray)
    }
    while(resultSet.next()) {
      val record = for ( i <- 1 to columnCount) yield { resultSet.getString(i) }
      recordCounter += 1
      csvWriter.writeNext(record.toArray)
    }
    buffer.close()
    AppLogger info s"exported $recordCounter rows to ${exportSetting.file.getPath}"
    recordCounter
  }

}

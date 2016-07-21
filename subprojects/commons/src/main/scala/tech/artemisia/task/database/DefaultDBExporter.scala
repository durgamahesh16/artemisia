package tech.artemisia.task.database

import java.io.OutputStream
import java.net.URI

import tech.artemisia.inventory.io.CSVFileWriter
import tech.artemisia.task.settings.ExportSetting

/**
 * Created by chlr on 7/10/16.
 */
trait DefaultDBExporter extends DBExporter {

self: DBInterface =>

  override def export(sql: String, outputStream: OutputStream, exportSetting: ExportSetting): Long = {
    val resultSet = self.query(sql)
    var recordCounter = 0L
    val csvWriter = new CSVFileWriter(outputStream, exportSetting)
    val columnCount = resultSet.getMetaData.getColumnCount
    if (exportSetting.header) {
      val header = for (i <- 1 to columnCount) yield { resultSet.getMetaData.getColumnLabel(i) }
      csvWriter.writeRow(header.toArray)
    }
    while(resultSet.next()) {
      val record = for ( i <- 1 to columnCount) yield { resultSet.getString(i) }
      recordCounter += 1
      csvWriter.writeRow(record.toArray)
    }
    csvWriter.close()
    recordCounter
  }

  override def export(sql: String, location: URI, exportSetting: ExportSetting) = ???

}

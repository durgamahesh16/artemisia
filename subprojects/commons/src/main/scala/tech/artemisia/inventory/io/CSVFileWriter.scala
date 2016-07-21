package tech.artemisia.inventory.io

import java.io._
import com.opencsv.CSVWriter
import tech.artemisia.task.settings.ExportSetting

/**
 * Created by chlr on 5/2/16.
 */
class CSVFileWriter(outputStream: OutputStream, settings: ExportSetting) extends FileDataWriter {

  override var totalRows: Long = 0L

  private val writer = new CSVWriter(new BufferedWriter(new OutputStreamWriter(outputStream)), settings.delimiter,
    if (settings.quoting) settings.quotechar else CSVWriter.NO_QUOTE_CHARACTER, settings.escapechar)

  override def writeRow(row: Array[String]) = {
    totalRows += 1
    writer.writeNext(row)
  }

  override def writeRow(data: String) = ???

  override def close() = {
    writer.flush()
    writer.close()
  }

}

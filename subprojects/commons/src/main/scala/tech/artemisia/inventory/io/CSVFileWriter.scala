package tech.artemisia.inventory.io

import java.io.{BufferedWriter, File, FileWriter}

import com.opencsv.CSVWriter
import tech.artemisia.task.settings.ExportSetting

/**
 * Created by chlr on 5/2/16.
 */
class CSVFileWriter(settings: ExportSetting) extends FileDataWriter {

  override var totalRows: Long = 0L

  private val writer = new CSVWriter(new BufferedWriter(new FileWriter(new File(settings.file))), settings.delimiter,
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

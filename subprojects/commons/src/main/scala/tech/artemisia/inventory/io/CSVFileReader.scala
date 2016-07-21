package tech.artemisia.inventory.io

import java.io.{BufferedReader, InputStream, InputStreamReader}
import com.opencsv.CSVReader
import tech.artemisia.task.settings.LoadSetting
import scala.collection.JavaConverters._


/**
  *
  * @param inputStream inputstream to read data from
  * @param settings csv settings to be applied.
  */
class CSVFileReader(inputStream: InputStream, settings: LoadSetting) extends Iterator[Array[String]] {

  private var counter = 0L

  val reader = new CSVReader(new BufferedReader(new InputStreamReader(inputStream)), settings.delimiter,
    settings.quotechar, settings.escapechar, settings.skipRows).iterator().asScala

  def rowCounter = counter

  override def hasNext: Boolean = {
    reader.hasNext
  }

  override def next(): Array[String] = {
    counter += 1
    reader.next()
  }

}

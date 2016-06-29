package tech.artemisia.inventory.io

import java.io.{BufferedReader, File, FileReader}
import com.opencsv.CSVReader
import tech.artemisia.task.settings.LoadSettings
import scala.collection.JavaConverters._

/**
  * Created by chlr on 5/1/16.
  */


class CSVFileReader(settings: LoadSettings) extends Iterator[Array[String]] {

  private var counter = 0L

  val reader = new CSVReader(new BufferedReader(new FileReader(new File(settings.location))), settings.delimiter,
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

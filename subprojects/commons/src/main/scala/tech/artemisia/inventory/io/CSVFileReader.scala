package tech.artemisia.inventory.io

import java.net.URI
import java.nio.file.Paths

import com.opencsv.CSVReader
import tech.artemisia.task.settings.LoadSetting
import tech.artemisia.util.FileSystemUtil.{expandPathToFiles, mergeFileStreams}

import scala.collection.JavaConverters._

/**
  * Created by chlr on 5/1/16.
  */


class CSVFileReader(settings: LoadSetting) extends Iterator[Array[String]] {

  private var counter = 0L

  val reader = new CSVReader(CSVFileReader.makeReader(settings.location), settings.delimiter,
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

object CSVFileReader {

  def makeReader(location: URI) = {
    location.getScheme match {
      case "file" => mergeFileStreams(expandPathToFiles(Paths.get(location)))
      case _ => throw new UnsupportedOperationException(s"uri $location is not supported")
    }
  }

}

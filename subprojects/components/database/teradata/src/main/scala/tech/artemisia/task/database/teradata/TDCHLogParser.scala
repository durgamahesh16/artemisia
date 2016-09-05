package tech.artemisia.task.database.teradata

import java.io.{OutputStream, PrintWriter}

import org.apache.commons.exec.LogOutputStream

/**
  * Created by chlr on 8/30/16.
  */
class TDCHLogParser(stream: OutputStream) extends LogOutputStream {

  var rowsLoaded = 0L
  val pattern = "^[\\s]*Map output records=(\\d+)[\\s]*$".r
  val writer = new PrintWriter(stream, true)

  override def processLine(line: String, logLevel: Int): Unit = {
    line match {
      case pattern(rows) => rowsLoaded = rows.toLong
      case _ => ()
    }
    writer.println(line)
  }

}

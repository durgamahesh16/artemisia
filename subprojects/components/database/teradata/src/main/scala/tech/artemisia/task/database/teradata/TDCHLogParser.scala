package tech.artemisia.task.database.teradata

import java.io.OutputStream
import tech.artemisia.inventory.io.OutputLogParser

/**
  * Created by chlr on 8/30/16.
  */
class TDCHLogParser(stream: OutputStream) extends OutputLogParser(stream) {

  var rowsLoaded = 0L
  val pattern = "^[\\s]*Map output records=(\\d+)[\\s]*$".r

  override def parse(line: String) = {
    line match {
      case pattern(rows) => rowsLoaded = rows.toLong
      case _ => ()
    }
  }

}

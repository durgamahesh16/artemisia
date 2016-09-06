package tech.artemisia.task.hadoop.hive

import java.io.{OutputStream, PrintWriter}
import org.apache.commons.exec.LogOutputStream
import scala.collection.mutable

/**
  * Created by chlr on 8/6/16.
  */

class HQLExecuteParser(stream: OutputStream) extends LogOutputStream {

  val rowsLoaded = mutable.Map[String,Long]() withDefaultValue 0L
  val pattern1 = raw"(\d+) Rows loaded to (\w+)".r
  val pattern2 = raw"Table\s(\b.+\b)\s.+numRows=(\d+),.*".r
  val writer = new PrintWriter(stream, true)

  override def processLine(line: String, logLevel: Int): Unit = {
    line match {
      case pattern1(row, table) => rowsLoaded += (table -> (rowsLoaded(table) + row.toLong))
      case pattern2(table, row) => rowsLoaded += (table -> (rowsLoaded(table) + row.toLong))
      case _ => ()
    }
    writer.println(line)
  }

}

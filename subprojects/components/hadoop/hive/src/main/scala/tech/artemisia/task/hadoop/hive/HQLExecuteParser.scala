package tech.artemisia.task.hadoop.hive

import org.apache.commons.exec.LogOutputStream
import scala.collection.mutable

/**
  * Created by chlr on 8/6/16.
  */

class HQLExecuteParser extends LogOutputStream {

  val rowsLoaded = mutable.Map[String,Long]() withDefaultValue 0L
  val pattern1 = raw"(\d+) Rows loaded to (\w+)".r
  val pattern2 = raw"Table\s(\b.+\b)\s.+numRows=(\d+),.*".r


  override def processLine(line: String, logLevel: Int): Unit = {
    line match {
      case pattern1(table, row) => rowsLoaded + (table -> rowsLoaded(table)+row)
      case pattern2(table, row) => rowsLoaded + (table -> rowsLoaded(table)+row)
    }
  }

}

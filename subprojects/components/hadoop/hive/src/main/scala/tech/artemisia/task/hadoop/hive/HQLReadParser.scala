package tech.artemisia.task.hadoop.hive

import java.io.PrintWriter

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.exec.LogOutputStream

/**
  * Created by chlr on 8/6/16.
  */
class HQLReadParser(writer: PrintWriter) extends LogOutputStream {

  var header: String = _
  var row: String = _
  var counter = 0

  override def processLine(line: String, logLevel: Int): Unit = {
    counter match {
      case 0 => counter += 1; header = line
      case 1 => counter += 1; row = line
      case _ => ()
    }
    writer.println(line)
  }

  def getData = {
    val joined = header.split('\t') zip row.split('\t')
    joined.foldLeft(ConfigFactory.empty()) {
      (carry, input) => carry.withValue(input._1, ConfigValueFactory.fromAnyRef(input._2))
    }
  }

}

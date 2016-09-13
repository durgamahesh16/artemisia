package tech.artemisia.inventory.io

import java.io.{OutputStream, PrintWriter}

import org.apache.commons.exec.LogOutputStream

/**
  * Created by chlr on 9/11/16.
  */

/**
  * Implement this class parse logs from a external process.
  *
  * @param stream stream where data has to be relayed once it is parsed.
  */
abstract class OutputLogParser(stream: OutputStream) extends LogOutputStream {

  final val writer = new PrintWriter(stream, true)

  final override def processLine(line: String, logLevel: Int): Unit = {
    parse(line)
    writer.println(line)
  }

  /**
    * parse each line and perform any side-effecting operation necessary
    * @param line line to be parsed.
    */
  def parse(line: String): Unit

}

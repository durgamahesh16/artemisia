package tech.artemisia.task.database.teradata.tpt

import java.io.OutputStream

import tech.artemisia.inventory.io.OutputLogParser

/**
  * Created by chlr on 9/11/16.
  */

/**
  * This class parses the stdout of the TPT job and captures important job parameters from the log.
  * The job parameters includes.
  * $ - tpt job name
  * $ - rows sent
  * $ - rows applied to table
  * $ - rows in err1 table
  * $ - rows in err2 table
  * $ - duplicate rows
  * $ - rows in error file
  * @param stream stream where data has to be relayed once it is parsed.
  */
class TPTLoadLogParser(stream: OutputStream) extends OutputLogParser(stream) {

  private val jobIdRgx = ".*Job id is (\\b[\\w]*.?[\\w]+-[\\d]+\\b),.*".r
  private val jobLogFileRgx = s"Job log:[\\s]+(.+)".r
  private val rowsSentRgx = "tpt_writer: Total Rows Sent To RDBMS:[\\s]+(\\d+)".r
  private val rowsAppliedRgx = "tpt_writer: Total Rows Applied:[\\s]+(\\d+)".r
  private val rowsErr1Rgx = "tpt_writer: Total Rows in Error Table 1:[\\s]+(\\d+)".r
  private val rowsErr2Rgx = "tpt_writer: Total Rows in Error Table 2:[\\s]+(\\d+)".r
  private val rowsDuplicateRgx = "tpt_writer: Total Duplicate Rows:[\\s]+(\\d+)".r
  private val errorFileRowsRgx = "tpt_reader:.+(\\d+).*error rows sent to error file.*".r

  var jobId: String  = _
  var rowsSent: Long = 0
  var rowsApplied: Long = 0
  var rowsErr1: Long = 0
  var rowsErr2: Long = 0
  var rowsDuplicate: Long = 0
  var errorFileRows: Long = 0
  var jobLogFile: String = _

  override def parse(line: String): Unit = {
    line match {
      case jobIdRgx(x) => jobId = x
      case rowsSentRgx(x) => rowsSent = x.toLong
      case rowsAppliedRgx(x) => rowsApplied = x.toLong
      case rowsErr1Rgx(x) => rowsErr1 = x.toLong
      case rowsErr2Rgx(x) => rowsErr2 = x.toLong
      case rowsDuplicateRgx(x) => rowsDuplicate = x.toLong
      case errorFileRowsRgx(x) => errorFileRows = x.toLong
      case jobLogFileRgx(x) => jobLogFile = x
      case _ => ()
    }
  }

}

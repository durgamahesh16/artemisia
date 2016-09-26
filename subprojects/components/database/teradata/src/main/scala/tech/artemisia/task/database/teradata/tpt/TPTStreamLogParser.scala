package tech.artemisia.task.database.teradata.tpt

import java.io.OutputStream

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import tech.artemisia.inventory.io.OutputLogParser
import tech.artemisia.task.database.DBInterface
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.util.{Failure, Success, Try}


/**
  *
  */
class TPTStreamLogParser(stream: OutputStream)
  extends OutputLogParser(stream)
  with BaseTPTLogParser {


  private val errorFileRowsRgx = "tpt_reader:\\s\\b[\\w]+\\b\\s(\\d+)\\serror rows sent to error file.+".r
  private val appliedRowsRgx = "tpt_writer: Rows Inserted:\\s+(\\d+)".r
  private val jobIdRgx = ".*Job id is (\\b[\\w]*.?[\\w]+-[\\d]+\\b),.*".r
  private val jobLogFileRgx = s"Job log:[\\s]+(.+)".r

  var appliedRows: Long = 0
  var rejectedRows: Long = 0
  var errorTableRows: Long = 0
  var errorFileRows: Long = 0
  override var jobId: String = _
  override var jobLogFile: String = _


  /**
    * parse each line and perform any side-effecting operation necessary
    *
    * @param line line to be parsed.
    */
  override def parse(line: String): Unit = {
    line match {
      case errorFileRowsRgx(rows) => errorFileRows = rows.toLong
      case appliedRowsRgx(rows) => appliedRows = rows.toLong
      case jobIdRgx(x) => jobId = x
      case jobLogFileRgx(x) => jobLogFile = x
      case _ => ()
    }
  }

  override def toConfig = {
    ConfigFactory.empty()
      .withValue("applied", ConfigValueFactory.fromAnyRef(appliedRows))
      .withValue("error-file", ConfigValueFactory.fromAnyRef(errorFileRows))
      .withValue("error-table", ConfigValueFactory.fromAnyRef(errorTableRows))
      .withValue("rejected", ConfigValueFactory.fromAnyRef(errorTableRows + errorFileRows))
      .withValue("source", ConfigValueFactory.fromAnyRef(appliedRows + errorTableRows + errorFileRows))
  }


  def updateErrorTableCount(tableName: String)(implicit dBInterface: DBInterface) = {
    Try(dBInterface.queryOne(s"SELECT count(*) as cnt FROM ${tableName}_ET", printSQL = false).as[Long]("cnt")) match {
      case Success(cnt) => errorTableRows = cnt
      case Failure(th) => ()
    }
  }


}

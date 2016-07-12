package tech.artemisia.task.database.teradata

import java.sql.{SQLException, BatchUpdateException}
import scala.collection.JavaConverters._
import tech.artemisia.core.AppLogger
import tech.artemisia.task.database.{DefaultDBExporter, BatchDBImporter, BaseDBBatchWriter, DBInterface}
import tech.artemisia.task.settings.LoadSetting

/**
  * Created by chlr on 7/2/16.
  */

trait TeraDataTransporter extends BatchDBImporter with DefaultDBExporter {

  self: DBInterface =>

  def getBatchWriter(tableName: String, loadSetting: LoadSetting) = {
    new TeraDataTransporter.FastLoadDBBatchWriter(tableName, loadSetting, this)
  }

}

object TeraDataTransporter {

  /**
   * This DBWriter instance differs from the default DBWriter on how BatchUpdateException is handled
   *
   * @param tableName    name of the table
   * @param loadSettings load settings
   * @param dBInterface  database interface object
   */
  class FastLoadDBBatchWriter(tableName: String, loadSettings: LoadSetting, dBInterface: DBInterface)
    extends BaseDBBatchWriter(tableName, loadSettings, dBInterface) {

    val errorRecordHandler = new FastLoadErrorRecordHandler(tableName)
    private var errorCounter = 0
    dBInterface.connection.setAutoCommit(false)

    val ignoreErrorCodes = Seq(1145, 1156, 1154, 1248, 1148, 1159, 1162, 1160, 1147)
    val errorRecordCode = 1160 // error code available only during commit operation
    val displayErrorCode = Seq(1159) // error code available only during commit operation

    def processBatch(batch: Array[Array[String]]) = {
      try {
        for (row <- batch) {
          try{ stmt.clearParameters(); composeStmt(row); stmt.addBatch() } catch {
            case th: Throwable => errorWriter.writeRow(row)
          }
        }
        stmt.executeBatch()
      } catch {
        case th: BatchUpdateException => {
          errorCounter += th.getUpdateCounts.count(_ < 0)
          th.iterator.asScala foreach {
            case x: SQLException if ignoreErrorCodes contains x.getErrorCode => ()
            case x: SQLException => { AppLogger error x.getMessage; throw x }
            case x: Throwable => true
          }
        }
        case th: Throwable => {
          AppLogger error th.getMessage
          throw th
        }
      }
    }

    override  def close() = {
      try {
        dBInterface.connection.commit()
        dBInterface.connection.setAutoCommit(true)
      } catch {
        case th: SQLException => {
          th.iterator.asScala foreach {
            case x: SQLException => {
              if (displayErrorCode contains x.getErrorCode) {
                AppLogger warn x.getMessage
              }
              if (x.getErrorCode == errorRecordCode) {
                errorRecordHandler.parseException(x)
              }
              if (!(ignoreErrorCodes contains x.getErrorCode)) {
                AppLogger error x.getMessage
                throw x
              }
            }
            case x: Throwable => {
              AppLogger error x.getMessage
              throw x
            }
          }
        }
        case th: Throwable => {
          AppLogger error th.getMessage
          throw th
        }
      } finally {
        stmt.close()
        errorWriter.close()
        errorRecordHandler.close()
      }
    }

  }
}

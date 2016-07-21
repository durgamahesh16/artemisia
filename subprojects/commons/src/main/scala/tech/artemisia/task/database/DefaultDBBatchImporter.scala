package tech.artemisia.task.database

import java.sql.{BatchUpdateException, SQLException}

import tech.artemisia.task.settings.LoadSetting


/**
  * The Default implementation of BaseBatchDBWriter writer.
  *
  */
trait DefaultDBBatchImporter extends BatchDBImporter {

  self: DBInterface =>

  def getBatchWriter(tableName: String, loadSetting: LoadSetting) = {
    new DefaultDBBatchImporter.DefaultDBBatchImporter(tableName, loadSetting, this)
  }


}

object DefaultDBBatchImporter {

  class DefaultDBBatchImporter(tableName: String, loadSettings: LoadSetting, dbInterface: DBInterface)
    extends BaseDBBatchWriter(tableName, loadSettings, dbInterface) {

    def processBatch(batch: Array[Array[String]]) = {
      val (validRows: Array[Array[String]], invalidRows: Array[Array[String]]) = batch partition { x => x.length == tableMetadata.length }
      invalidRows foreach {  errorWriter.writeRow }

      try {
        for (row <- validRows) {
          try { composeStmt(row); stmt.addBatch() } catch { case th: Throwable =>  errorWriter.writeRow(row) }
        }
        stmt.executeBatch()
      }
      catch {
        case e: BatchUpdateException => {
          val results = e.getLargeUpdateCounts
          println(results.mkString(","))
          val retryRecords = results zip validRows filter { x => x._1 < 0 } map { _._2 }
          stmt.clearBatch()
          for(record <- retryRecords) {
            try { composeStmt(record); stmt.execute() }
            catch {
              case e: SQLException => errorWriter.writeRow(record)
            }
          }
        }
        case th: Throwable => ()
      }
      finally {
        stmt.clearParameters()
        stmt.clearBatch()
      }
    }
  }
}

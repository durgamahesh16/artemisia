package tech.artemisia.task.database

import java.sql.{BatchUpdateException, SQLException}

import tech.artemisia.task.settings.LoadSetting


/**
  * The Default implementation of BaseBatchDBWriter writer.
  *
  * @param tableName name of the table
  * @param loadSettings load settings
  * @param dBInterface database interface object
  */
class DefaultDBWriter(tableName: String, loadSettings: LoadSetting, dBInterface: DBInterface)
  extends BaseDBWriter(tableName, loadSettings, dBInterface) {


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

  override def processRow(row: Array[String]): Unit = ???

}

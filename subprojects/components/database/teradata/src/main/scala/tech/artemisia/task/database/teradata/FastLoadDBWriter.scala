package tech.artemisia.task.database.teradata

import java.sql.{BatchUpdateException, SQLException}

import tech.artemisia.core.AppLogger
import tech.artemisia.task.database.{BaseDBWriter, DBInterface}
import tech.artemisia.task.settings.LoadSetting

import scala.collection.JavaConverters._

/**
  * This DBWriter instance differs from the default DBWriter on how BatchUpdateException is handled
  *
  * @param tableName    name of the table
  * @param loadSettings load settings
  * @param dBInterface  database interface object
  */
class FastLoadDBWriter(tableName: String, loadSettings: LoadSetting, dBInterface: DBInterface)
  extends BaseDBWriter(tableName, loadSettings, dBInterface) {

  AppLogger debug "setting autocommit to false"

  stmt.getConnection.setAutoCommit(false)

  def processBatch(batch: Array[Array[String]]) = {
    try {
      for (row <- batch) {
        try{ composeStmt(row); stmt.addBatch() } catch { case th: Throwable => errorWriter.writeRow(row) }
      }
      val cnt = stmt.executeBatch()
      println(cnt.mkString(","))
    } catch {
      case th: BatchUpdateException => {
        println("error update cnt: "+th.getUpdateCounts.mkString(","))
        AppLogger error th.iterator.asScala.toList.last.getMessage
      }
      case th: Throwable => {
        AppLogger error th.getMessage
      }
    }
  }

  override def processRow(row: Array[String]): Unit = ???

  override  def close() = {
    try {
      persistErrorTable(s"${tableName}_ERR_1")
      persistErrorTable(s"${tableName}_ERR_2")
      stmt.getConnection.commit()
      stmt.getConnection.setAutoCommit(true)
    } catch {
      case th: SQLException => {

        AppLogger error th.iterator.asScala.toList.last.getMessage
      }
      case th: Throwable => { AppLogger error s"Fastload failed because of error: ${th.getMessage}" }
    } finally {
      stmt.close()
      errorWriter.close()
    }
  }

  private def persistErrorTable(table: String) = {
    try {
      val rs = dBInterface.connection.prepareStatement(s"locking row for access SELECT count(*) as cnt FROM $table").executeQuery()
      while(rs.next()) {
        println(s"error row count for table $table is ${rs.getInt(1)}")
      }
      rs.close()
    } catch {
      case th: Throwable => println(s"error querying error table ${th.getMessage}")
    }
  }

  private def getErrorMessage(th: Throwable) = {
    val rgx = """[.*] [.*] [.*] (.*)""".r
  }
}

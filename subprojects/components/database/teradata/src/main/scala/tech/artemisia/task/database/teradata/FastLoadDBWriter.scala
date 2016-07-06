package tech.artemisia.task.database.teradata

import java.sql.{BatchUpdateException, SQLException}

import tech.artemisia.core.AppLogger
import tech.artemisia.task.database.DBUtil.executeUpdateQuery
import tech.artemisia.task.database.{BaseDBWriter, DBInterface}
import tech.artemisia.task.settings.LoadSetting

import scala.collection.JavaConverters._
import scala.util.Failure

/**
  * This DBWriter instance differs from the default DBWriter on how BatchUpdateException is handled
  *
  * @param tableName    name of the table
  * @param loadSettings load settings
  * @param dBInterface  database interface object
  */
class FastLoadDBWriter(tableName: String, loadSettings: LoadSetting, dBInterface: DBInterface)
  extends BaseDBWriter(tableName, loadSettings, dBInterface) {

  dBInterface.connection.setAutoCommit(false)

  implicit val supportConnection = dBInterface.getNewConnection
  private val uvTable = s"${tableName}_uv"
  private val etTable = s"${tableName}_et"
  private val originalETTable = s"${tableName}_ERR_1"
  private val originalUVTable = s"${tableName}_ERR_2"


  override def postLoad() = {
    if (getTableRowCount(originalETTable) > 0) {
      AppLogger debug s"persisting ET table in $etTable"
      createETTable()
      persistETTable()
    }
    if (getTableRowCount(originalUVTable) > 0) {
      AppLogger debug s"persisting UV table in $uvTable"
      createUVTable()
      persistUVTable()
    }
  }

  def processBatch(batch: Array[Array[String]]) = {
    try {
      for (row <- batch) {
        try{ composeStmt(row); stmt.addBatch() } catch { case th: Throwable => errorWriter.writeRow(row) }
      }
      val cnt = stmt.executeBatch()
    } catch {
      case th: BatchUpdateException => {
        println(s"begin batch exception chain")
        th.iterator.asScala foreach {
           case x: SQLException => println(s"${x.getErrorCode} and ${x.getSQLState}") ;AppLogger warn x.getMessage
           case _ => ()
        }
        println(s"end batch exception chain")
      }
      case th: Throwable => {
        AppLogger warn th.getMessage
      }
    }
  }

  override def processRow(row: Array[String]): Unit = ???

  override  def close() = {
    try {
      dBInterface.connection.commit()
      dBInterface.connection.setAutoCommit(true)
    } catch {
      case th: SQLException => {
        println(s"begin commit exception chain")
        th.iterator.asScala foreach { x => println(s"${x.getClass.getName}") ;AppLogger warn x.getMessage }
        println(s"end commit exception chain")
      }
      case th: Throwable =>  AppLogger error s"Fastload failed because of error: ${th.getMessage}"
    } finally {
      stmt.close()
      errorWriter.close()
    }
  }

  private def createETTable() = {
    val dropTable = s"DROP TABLE $etTable"
    val createTable =
      s"""
         |CREATE MULTISET TABLE $etTable ,FALLBACK ,
         |     NO BEFORE JOURNAL,
         |     NO AFTER JOURNAL,
         |     CHECKSUM = DEFAULT,
         |     DEFAULT MERGEBLOCKRATIO
         |     (
         |      ErrorCode INTEGER FORMAT 'ZZZZ9',
         |      ErrorFieldName VARCHAR(120) CHARACTER SET UNICODE NOT CASESPECIFIC,
         |      DataParcel VARBYTE(64000))
         |PRIMARY INDEX ( DataParcel );
      """.stripMargin
      executeUpdateQuery(dropTable)
    executeUpdateQuery(createTable) match {
      case Failure(th) => throw th
      case _ => ()
    }
  }

  private def createUVTable() = {
    val dropTable = s"DROP TABLE $uvTable"
    val createTable =
      s"""
        | CREATE TABLE $uvTable AS $tableName WITH NO DATA NO PRIMARY INDEX;
      """.stripMargin
    executeUpdateQuery(dropTable)
    executeUpdateQuery(createTable) match {
      case Failure(th) => throw th
      case _ => ()
    }
  }


  private def persistETTable() = {
      val sql =
        s"""
           |LOCKING TABLE $originalETTable FOR ACCESS
           |INSERT INTO $etTable
           |SELECT * FROM $originalETTable
         """.stripMargin
      executeUpdateQuery(sql)
  }

  private def persistUVTable() = {
     val  sql =
       s"""
          |LOCKING TABLE $originalUVTable FOR ACCESS
          |INSERT INTO $uvTable
          |SELECT * FROM $originalUVTable
        """.stripMargin
  }

  private def getTableRowCount(table: String) = {
    val sql =
      s"""LOCKING ROW FOR ACCESS
         |SELECT count(*) as cnt FROM $table""".stripMargin
    try {
      val stmt = supportConnection.prepareStatement(sql)
      println(sql)
      val rs = stmt.executeQuery()
      rs.next()
      val result = rs.getInt(1)
      println(s"table $table row count is $result")
      result
    } catch {
      case th: SQLException => { println(s"${th.getMessage}")  ;0}
    } finally {
      stmt.close()
    }
  }

}

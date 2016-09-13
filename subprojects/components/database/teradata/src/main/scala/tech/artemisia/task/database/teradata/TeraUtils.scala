package tech.artemisia.task.database.teradata

import java.io.ByteArrayOutputStream
import java.sql.SQLException
import tech.artemisia.util.CommandUtil._
import tech.artemisia.core.AppLogger._
import tech.artemisia.task.database.{DBInterface, DBUtil}
import scala.util.{Failure, Success, Try}

/**
  * Created by chlr on 7/31/16.
  */

/**
  * A collection of utility methods
  */
object TeraUtils {

  /**
    * utility method to drop and recreate a table
    * @param tableName name of the table
    * @param dbInterface implicit dbinterface object
    */
  def dropRecreateTable(tableName: String)(implicit dbInterface: DBInterface): Unit = {
    info("dropping and recreating the table")
    val rs = dbInterface.query(s"SHOW TABLE $tableName", printSQL = false)
    val ct_stmt = { rs.next() ;val res = rs.getString(1); rs.close(); res }
    dbInterface.execute(s"DROP TABLE $tableName", printSQL = false)
    dbInterface.execute(ct_stmt, printSQL = false)
  }


  /**
    * truncate the table and if truncate fails due to fastload lock drop and re-create the table.
    * @param tableName name of the table.
    * @param dBInterface
    */
  def truncateElseDrop(tableName: String)(implicit dBInterface: DBInterface): Unit = {
    info("attempting to truncate table")
    Try(dBInterface.execute(s"DELETE FROM $tableName")) match {
      case Success(_) => ()
      case Failure(th: SQLException)
        if th.getErrorCode == 2652  => dropRecreateTable(tableName)
      case Failure(x) => throw x
    }
  }


  /**
    *
    * @param loadSize size of data in bytes
    * @param loadSetting input load setting
    * @return customized final load setting
    */
  def overrideLoadSettings(loadSize: Long,loadSetting: TeraLoadSetting) = {
    if (loadSize > loadSetting.bulkLoadThreshold) {
      loadSetting.copy(batchSize = 80000, mode = "fastload")
    }
    else {
      loadSetting.copy(batchSize = 1000, mode = "default")
    }
  }


  /**
    * fetch table column metadata details
    * @param databaseName databasename
    * @param tableName target table name
    * @param dBInterface Teradata DBInterface instance
    * @return
    */
  def tableMetadata(databaseName: String, tableName: String, dBInterface: DBInterface) = {
    val sql =
      s"""
        |SELECT
        |ROW_NUMBER() over(order by ColumnId) as row_id
        |,TRIM(ColumnName)
        |,ColumnType
        |,(CASE
        |  WHEN Columntype = 'DA' THEN 15
        |  WHEN ColumnType = 'D' THEN (decimaltotaldigits + 5)
        |  WHEN ColumnType IN ('BV','PM','SZ') THEN ColumnLength
        |  WHEN ColumnType IN ('TS','TZ') THEN 35
        |  WHEN ColumnType IN ('I ','I2','I1','I8','F') THEN 25 ELSE ColumnLength END ) AS byteLength
        |,TRIM(SUBSTR(ColumnName,1,23))||'_'||CAST(row_id as Varchar(5))||'_' as SafeColumnName
        |,Nullable
        |FROM DBC.Columns c WHERE DatabaseName = '$databaseName' AND TableName = '$tableName'  ORDER BY ColumnId
      """.stripMargin
    val rs = dBInterface.query(sql = sql, printSQL = false)
    val resultSetIterator = new DBUtil.ResultSetIterator[(String, String, Short, String, String)](rs) {
      override def generateRow: (String, String, Short, String, String) = {
        (resultSet.getString(2), resultSet.getString(3), resultSet.getShort(4), resultSet.getString(5), resultSet.getString(6))
      }
    }
    resultSetIterator.toList
  }

  /**
    *
    * This is implemented by running twbstat command and parsing the output.
    * @param jobName tpt job name
    * @return
    */
  def detectTPTRun(jobName: String): Seq[String] = {
    val stream = new ByteArrayOutputStream()
    assert(executeCmd(Seq("twbstat"), stdout =stream) == 0, "twbstat command failed. ensure TPT is properly installed")
    val content = new String(stream.toByteArray)
    val rgx = s"$jobName-[\\d]+".r
    content.split(System.lineSeparator())
      .map(_.trim)
      .filter(rgx.findFirstMatchIn(_).isDefined)
  }


  /**
   *
   * @param twbKillBin path string for twbkill binary
   * @param jobName
   */
  def killTPTJob(twbKillBin: String, jobName: String) = {
    val cmd = Seq(twbKillBin, jobName)
    executeCmd(cmd)
  }


}

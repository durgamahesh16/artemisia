package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.database.DBUtil.ResultSetIterator
import tech.artemisia.core.AppLogger._
import tech.artemisia.util.Util

import scala.io.Source
import scala.util.{Failure, Success, Try}


/**
  * Created by chlr on 9/24/16.
  */


/**
  * abstract TPTErrorLogger class
  */
trait TPTErrorLogger {

  /**
    * target table name. the error table names are derieved from the target table name.
    */
  protected val tableName: String

  /**
    * location of the error file
    */
  protected val errorFile: String

  /**
    * log error
    */
  def log(): Unit


  /**
    * db-interface
    */
  protected val dbInterface: DBInterface


  /**
    * error file content.
    */
  protected lazy val errorFileContent = {
    Source.fromFile(errorFile).getLines.take(10)
  }

}

object TPTErrorLogger {


  def createErrorLogger(tableName: String,
                        errorFile: String,
                        dbInterface: DBInterface,
                        mode: String) = {
    mode match {
      case "default" => new TPTStreamOperErrLogger(tableName, errorFile, dbInterface)
      case "fastload" => new TPTLoadOperErrLogger(tableName, errorFile, dbInterface)
      case x => throw new RuntimeException(s"mode $x is not supported.")
    }
  }

  /**
    * Error Info logger for load operator
    *
    * @param tableName
    * @param errorFile
    */
  class TPTLoadOperErrLogger(override protected val tableName: String
                            ,override protected val errorFile: String
                            ,override protected val dbInterface: DBInterface)
    extends TPTErrorLogger {

    protected lazy val etTableContent: Seq[(String, String, String)] = {
      Seq(("FieldName", "Rowcount", "ErrorMessage")) ++ fetchData {
        s"""|LOCKING ROW FOR ACCESS
            |SELECT
            |ErrorFieldName as Fields,
            |count(*) as cnt,
            |ErrorText as ErrorMessage
            |FROM ${tableName}_ET t0
            |INNER JOIN dbc.errormsgs t1
            |ON t0.ErrorCode = t1.ErrorCode
            |GROUP BY 1,3;""".stripMargin
      }
    }

    final def fetchData(query: String) = {
      Try(dbInterface.query(query, printSQL = false)) match {
        case Failure(th) => Seq[(String,String, String)]()
        case Success(rs) =>
          val resultSetIterator = new ResultSetIterator[(String, String, String)](rs) {
            override def generateRow: (String, String, String) = {
              (resultSet.getString(1), resultSet.getString(2), resultSet.getString(3))
            }
          }
          resultSetIterator.toSeq
      }
    }


    /**
      * log error
      */
    override def log(): Unit = {
      if (etTableContent.length > 1) {
        info("printing _ET table content")
        val table = Util.prettyPrintAsciiTable(etTableContent.map(x => Array(x._1, x._2, x._3)).toArray)
        info(s"\n\n${table.mkString(System.lineSeparator())}\n")
      }
      if (errorFileContent.nonEmpty) {
        val message = ("printing first 10 lines in $errorFile" +: errorFileContent.toSeq)
          .mkString(System.lineSeparator())
        info(s"$message\n")
      }
    }
  }


  class TPTStreamOperErrLogger(override protected val tableName: String
                               ,override protected val errorFile: String
                               ,override protected val dbInterface: DBInterface)
    extends TPTErrorLogger {


    protected lazy val etTableContent: Seq[(String, String)] = {
      Seq(("ErrorMessage", "Rowcount")) ++ fetchData {
        s"""
           |SELECT ErrorMsg,count(*) AS cnt
           |FROM ${tableName}_ET
           |GROUP BY 1
           |ORDER BY 2 desc;
         """.stripMargin
      }
    }


    final def fetchData(query: String) = {
      Try(dbInterface.query(query, printSQL = false)) match {
        case Failure(th) => Seq[(String, String)]()
        case Success(rs) =>
          val resultSetIterator = new ResultSetIterator[(String, String)](rs) {
            override def generateRow: (String, String) = {
              (resultSet.getString(1), resultSet.getString(2))
            }
          }
          resultSetIterator.toSeq
      }
    }


    /**
      * log error
      */
    override def log(): Unit = {
      if (etTableContent.length > 1) {
        info("printing _ET table content")
        val table = Util.prettyPrintAsciiTable(etTableContent.map(x => Array(x._1, x._2)).toArray)
        info(s"\n\n${table.mkString(System.lineSeparator())}\n")
      }
      if (errorFileContent.nonEmpty) {
        val message = ("printing first 10 lines in $errorFile" +: errorFileContent.toSeq)
          .mkString(System.lineSeparator())
        info(s"$message\n")
      }
    }
  }

}

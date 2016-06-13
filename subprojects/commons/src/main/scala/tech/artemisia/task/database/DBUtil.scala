package tech.artemisia.task.database

import java.sql.ResultSet

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by chlr on 4/15/16.
  */

object DBUtil {


  /**
   * takes a tablename literal and parses the optional databasename part and the tablename part
    *
    * @param tableName input table of format <databasename>.<tablename>
   * @return
   */
  def parseTableName(tableName: String): (Option[String],String) = {
      val result  = tableName.split("""\.""").toList
      result.length match {
        case 1 => None -> result.head
        case 2 => Some(result.head) -> result(1)
        case _ => throw new IllegalArgumentException(s"$tableName is a valid table identifier")
      }
  }


  /**
    *
    * @todo convert java.sql.Time to Duration type in Hocon
    * @param rs input ResultSet
    * @return config object parsed from first row
    */
  def resultSetToConfig(rs: ResultSet) = {
    if(rs.next()) {
      val result = for(i <- 1 to rs.getMetaData.getColumnCount) yield {
        rs.getMetaData.getColumnName(i) -> rs.getObject(i)
      }
      result.foldLeft[Config](ConfigFactory.empty()) {
        (config: Config, data: (String, Object)) => {
          val parsed = data match {
            case (x, y: String) =>  s" { $x = $y }"
            case (x, y: java.lang.Number) => s" { $x = ${y.toString} }"
            case (x, y: java.lang.Boolean) => s"{ $x = ${if (y) "yes" else "no"} }"
            case (x, y: java.util.Date) => s"""{ $x = "${y.toString}" }"""
            case _ => throw new UnsupportedOperationException(s"Type ${data._2.getClass.getCanonicalName} is not supported")
          }
          ConfigFactory parseString parsed withFallback config
        }
      }
    }
    else {
      ConfigFactory.empty()
    }
  }


  /**
    *
    * @param rs input ResultSet
    * @param header include header
    * @return Stream of records represented as array of columns
    */
  def streamResultSet(rs: ResultSet, header: Boolean = false) = {
    val columnCount = rs.getMetaData.getColumnCount

    def nextRecord: Stream[Array[String]] = {
      if (rs.next()) {
        val record = for ( i <- 1 to columnCount) yield { rs.getString(i) }
        Stream.cons(record.toArray,nextRecord)
      } else {
        Stream.empty
      }
    }

    if (header) {
      val headerRow = for (i <- 1 to columnCount) yield { rs.getMetaData.getColumnLabel(i) }
      Stream.cons(headerRow.toArray, nextRecord)
    }
    else
      nextRecord
  }


 }

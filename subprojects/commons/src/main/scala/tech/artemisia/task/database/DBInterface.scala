package tech.artemisia.task.database


import java.io.{InputStream, OutputStream}
import java.net.URI
import java.sql.{Connection, ResultSet}
import com.typesafe.config.Config
import tech.artemisia.core.AppLogger
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}
import tech.artemisia.util.Util


/**
 * Created by chlr on 4/13/16.
 */


/**
 * A standard database interface for common operations such as
 *   - query result
 *   - execute DML statements
 *   - query and parse and store results HoconConfig objects
 *   - close connection
 */
trait DBInterface {

  self: DBImporter with DBExporter =>

  /**
   *  JDBC connection object
   */
  final lazy val connection: Connection = getNewConnection

  /**
    * creates a new connection object
    *
    * @return JDBC connection object
    */
  def getNewConnection: Connection

  /**
   *
   * @param sql Select query to be executed
   * @return result object
   */
  def query(sql: String, printSQL: Boolean = true): ResultSet = {
    if(printSQL)
        AppLogger info Util.prettyPrintAsciiBanner(sql,"query")
    val stmt = connection.prepareStatement(sql)
    stmt.executeQuery()
  }

  /**
   *
   * @param sql DML query to be executed
   * @return number of records updated/deleted/inserted
   */
  def execute(sql: String, printSQL: Boolean = true): Long = {
    if (printSQL) {
      AppLogger info "executing query"
      AppLogger info Util.prettyPrintAsciiBanner(sql, "query")
    }
    val stmt = connection.prepareStatement(sql)
    val recordCnt = stmt.executeUpdate()
    stmt.close()
    recordCnt
  }

  /**
   *
   * @param sql Select query to be executed
   * @return Hocon Config object of the first record
   */
  def queryOne(sql: String): Config = {
    AppLogger info "executing query"
    AppLogger info Util.prettyPrintAsciiBanner(sql,"query")
    val stmt = connection.prepareStatement(sql)
    val rs = stmt.executeQuery()
    val result = DBUtil.resultSetToConfig(rs)
    try {
      rs.close()
      stmt.close()
    }
    catch {
      case e: Throwable => {
        AppLogger warn e.getMessage
      }
    }
    result
  }

  /**
    * export query result to file
    *
    * @param sql query
    * @param exportSetting export settings
    * @return no of records exported
    */
  def exportSQL(sql: String, target: Either[OutputStream, URI], exportSetting: ExportSetting): Long = {
    target match {
      case Left(outputStream) => self.export(sql, outputStream, exportSetting)
      case Right(location) => self.export(sql, location, exportSetting)
    }
  }

  /**
   * Load data to a table typically from a file.
   *
   * @param tableName destination table
   * @param loadSettings load settings
   * @return tuple of total records in source and number of records rejected
   */
  def loadTable(tableName: String, source: Either[InputStream,URI] , loadSettings: LoadSetting) = {
      val (total,rejected) = source match {
        case Left(inputStream) => self.load(tableName, inputStream, loadSettings)
        case Right(location) => self.load(tableName, location, loadSettings)
      }
      loadSettings.errorTolerance foreach {
        val errorPct = (rejected.asInstanceOf[Float] / total) * 100
        x => assert( errorPct < x , s"Load Error % ${"%3.2f".format(errorPct)} greater than defined limit: ${x * 100}")
      }
    total -> rejected
  }

  /**
   * close the database connection
   */
  def terminate(): Unit = {
    connection.close()
  }

  /**
   *
   * @param databaseName databasename
   * @param tableName tablename
   * @return Iterable of Tuple of name and type of the column
   */
  def getTableMetadata(databaseName: Option[String] ,tableName: String): Iterable[(String,Int)] = {
    val effectiveTableName = (databaseName map {x => s"$x.$tableName"}).getOrElse(tableName)
    val sql = s"SELECT * FROM $effectiveTableName"
    val rs = this.query(sql, printSQL = false)
    val metadata = rs.getMetaData
    val result = for (i <- 1 to metadata.getColumnCount) yield {
      metadata.getColumnName(i) -> metadata.getColumnType(i)
    }
    rs.close()
    result
  }

}









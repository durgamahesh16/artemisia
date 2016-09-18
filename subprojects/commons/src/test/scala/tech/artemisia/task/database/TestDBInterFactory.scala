package tech.artemisia.task.database

import java.sql.{Connection, DriverManager}
import tech.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 4/27/16.
 */
object TestDBInterFactory {
  
  
  def withDefaultDataLoader(table: String, database: String = "test",mode: Option[String] = None, createTestTable: Boolean = true) = {
    val dbInterface: DBInterface = new DBInterface with DefaultDBBatchImporter with DefaultDBExporter  {
      override def getNewConnection: Connection = {
        val modeOption = (mode map { x => s"MODE=$x;" }).getOrElse("")
        Class.forName("org.h2.Driver")
        DriverManager.getConnection(s"jdbc:h2:mem:$database;${modeOption}DB_CLOSE_DELAY=-1","","")
      }
    }
    processDbInterface(dbInterface, table)
    dbInterface
  }
  
  
  private def processDbInterface(dbInterface: DBInterface, table: String) = {
    dbInterface.execute(
      s"""CREATE TABLE IF NOT EXISTS $table
         |(
         | col1 int,
         | col2 varchar(10),
         | col3 boolean,
         | col4 tinyint,
         | col5 bigint,
         | col6 decimal(6,2),
         | col7 time,
         | col8 date,
         | col9 timestamp,
         |)
         |""".stripMargin)
    dbInterface.execute(s"DELETE FROM $table")
    dbInterface.execute(s"INSERT INTO $table VALUES (1, 'foo', true, 100, 10000000, 87.3, '12:30:00', '1945-05-09', '1945-05-09 12:30:00')")
    dbInterface.execute(s"INSERT INTO $table VALUES (2, 'bar', false, 100, 10000000, 8723.38, '12:30:00', '1945-05-09', '1945-05-09 12:30:00')")
  }

  /**
   * This is stubbed ConnectionProfile object primarily to be used along with H2 database which doesn't require a connectionProfile object to work with.
   * @return ConnectionProfile object
   */
  val stubbedConnectionProfile = DBConnection("","","","",-1)

}



package tech.artemisia.task.database.mysql

import tech.artemisia.TestSpec
import tech.artemisia.task.database.TestDBInterFactory
import tech.artemisia.task.settings.{DBConnection, BasicExportSetting}
import tech.artemisia.util.FileSystemUtil

import scala.io.Source

/**
  * Created by chlr on 6/2/16.
  */
class MySQLTaskSpec extends TestSpec {

  "MySQLTask" must "execute dml queries correctly" in {
    val table = "mysql_dummy_table1"
    val taskName = "SQLExecuteTest"
    val sqlExecute = new SQLExecute(taskName, s"delete from $table" ,DBConnection("","","","",10)) {
        override val dbInterface = TestDBInterFactory.withDefaultDataLoader(table,Some("mysql"))
    }
    val result = sqlExecute.execute()
    result.getInt(s"$taskName.__stats__.updated") must be (2)
  }

  it must "export query ouput" in {
    val table = "mysql_dummy_table2"
    val taskName = "SQLExportTest"
    FileSystemUtil.withTempFile(fileName = table) {
      file => {
        val task = new ExportToFile(taskName, s"select * from $table", file.toURI ,DBConnection("","","","",10), BasicExportSetting()) {
          override val dbInterface = TestDBInterFactory.withDefaultDataLoader(table,Some("mysql"))
        }
        val result = task.execute()
        result.getInt(s"$taskName.__stats__.rows") must be (2)
        Source.fromFile(file).getLines().toList.head must be ("1,foo,TRUE,100,10000000,87.30,12:30:00,1945-05-09,1945-05-09 12:30:00.0")
      }
    }
  }

  it must "read sql query and emit config value" in {
    val table = "mysql_dummy_table3"
    val taskName = "SQLReadTest"
    val sqlRead = new SQLRead(taskName, s"select col1 from $table where col2 = 'foo'" ,DBConnection("","","","",10)) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(table,Some("mysql"))
    }
    val result = sqlRead.execute()
    result.getInt("COL1") must be (1)
  }
  

}

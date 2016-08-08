package tech.artemisia.task.hadoop.hive

import tech.artemisia.TestSpec
import tech.artemisia.task.database.{BasicExportSetting, TestDBInterFactory}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 8/7/16.
  */
class HiveServerInterfaceSpec extends TestSpec {

  "HiveServerDBInterface" must "execute query" in {
    val tableName = "hive_table_execute"
    val task = new HQLExecute("hql_execute", s"delete from $tableName", Some(DBConnection.getDummyConnection)) {
      override lazy val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("hql_execute.__stats__.updated") must be (2)
  }

  it must "export query" in {
    val tableName = "hive_table_export"
    withTempFile(fileName = tableName) {
      file =>
        val task = new HQLExport("hql_execute", s"select * from $tableName", file.toURI
          ,DBConnection.getDummyConnection , BasicExportSetting()) {
          override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
        }
        val result = task.execute()
        result.getInt(s"hql_execute.__stats__.rows") must be (2)
    }
  }

  it must "support sqlread" in {
    val tableName = "hive_table_sqlread"
    val task = new HQLRead("hql_execute", s"select count(*) as cnt from $tableName", Some(DBConnection.getDummyConnection)) {
      override lazy val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("CNT") must be (2)
  }

 }

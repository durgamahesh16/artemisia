package tech.artemisia.task.database.teradata

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.net.URI

import tech.artemisia.TestSpec
import tech.artemisia.task.database.{BasicExportSetting, TestDBInterFactory}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.FileSystemUtil.{FileEnhancer, withTempFile}
/**
  * Created by chlr on 7/12/16.
  */
class TeraLoaderSpec extends TestSpec {

  "TeradataComponent" must "load data to table" in {
    val tableName = "td_load_test"
    withTempFile(fileName = "td_load_test") {
      file =>
        file <<=
          """ |100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |101,bravo,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |102,whiskey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |103,blimey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |104,victor,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00 """.stripMargin

          val loader = new LoadFromFile(taskName = "td_load_test",tableName = tableName,location = file.toURI,
            connectionProfile = DBConnection("", "", "", "", -1),
            loadSetting = TeraLoadSetting()) {
            override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
            override val source: Either[InputStream, URI] = Left(new BufferedInputStream(new FileInputStream(file)))
          }
        loader.supportedModes must be === "fastload" :: "default" :: "auto" :: Nil
       val result = loader.execute()
        result.getInt("td_load_test.__stats__.loaded") must be (5)
    }
  }

  it must "export data to file" in {
    val tableName = "td_export_test"
    withTempFile(fileName = tableName) {
      file =>
        val export = new ExportToFile(name = tableName, sql = s"SELECT * FROM $tableName", file.toURI
          , connectionProfile = DBConnection("", "", "", "", -1),
          exportSetting = BasicExportSetting()) {
          override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
        }
        export.supportedModes must be === "default" :: "fastexport"  :: Nil
        val result = export.execute()
        result.getInt("td_export_test.__stats__.rows") must be (2)
    }
  }


  it must "execute DML statement" in {
    val tableName = "td_execute"
    val execute = new SQLExecute(name = tableName, sql = s"DELETE FROM $tableName", connectionProfile = DBConnection("", "", "", "", -1)) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = execute.execute()
    result.getInt("td_execute.__stats__.updated") must be (2)
  }

  it must "run SQLRead task" in {
    val tableName = "td_read"
    val execute = new SQLRead(name = tableName, sql = s"SELECT col2 FROM $tableName WHERE col1 = 1",
      connectionProfile = DBConnection("", "", "", "", -1)) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = execute.execute()
    result.getString("COL2") must be ("foo")
  }


}

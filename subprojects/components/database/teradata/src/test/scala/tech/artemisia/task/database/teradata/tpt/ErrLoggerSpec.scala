package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.TestSpec
import TPTErrorLogger._
import tech.artemisia.task.database.TestDBInterFactory
/**
  * Created by chlr on 9/26/16.
  */
class ErrLoggerSpec extends TestSpec {

  "LoadOperErrLogger" must "handle load operator error logger" in {
    val dummy_table = "target_table"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(dummy_table)
    val errorFile = this.getClass.getResource("/errorfile.txt").getFile
    val errlogger = new LoadOperErrLogger(tableName=dummy_table, errorFile = errorFile, dbInterface = dbInterface) {
      override val errorSql =  s"SELECT col2, col1, col2 FROM $dummy_table"
      etTableContent must have length 3
      etTableContent.head must be ("FieldName","Rowcount","ErrorMessage")
      etTableContent(1) must be ("foo", "1" ,"foo")
      etTableContent(2) must be ("bar", "2" ,"bar")
      errorFileContent.head must be ("col1,data1")
      errorFileContent(1) must be ("col2,data2")
      errorFileContent(2) must be ("col3,data3")
    }
    errlogger.log()
  }

  "StreamOperErrLogger" must "handle stream operator error logger" in {

    val dummy_table = "target_table"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(dummy_table)
    val errorFile = this.getClass.getResource("/errorfile.txt").getFile
    val errlogger = new StreamOperErrLogger(tableName=dummy_table, errorFile = errorFile, dbInterface = dbInterface) {
      override val errorSql =  s"SELECT col2, col1 FROM $dummy_table"
      etTableContent must have length 3
      etTableContent.head must be ("ErrorMessage", "Rowcount")
      etTableContent(1) must be ("foo", "1")
      etTableContent(2) must be ("bar", "2")
      errorFileContent.head must be ("col1,data1")
      errorFileContent(1) must be ("col2,data2")
      errorFileContent(2) must be ("col3,data3")
    }
    errlogger.log()
  }

}

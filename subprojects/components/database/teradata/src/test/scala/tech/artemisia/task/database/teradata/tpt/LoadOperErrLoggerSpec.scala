package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.TestSpec
import TPTErrorLogger._
import tech.artemisia.task.database.TestDBInterFactory
/**
  * Created by chlr on 9/26/16.
  */
class LoadOperErrLoggerSpec extends TestSpec {

  "LoadOperErrLogger" must "" in {
    val dummy_table = "target_table"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(dummy_table)
    val errorFile = this.getClass.getResource("/errorfile.txt").getFile
    val errLogger = new LoadOperErrLogger(tableName=dummy_table, errorFile = errorFile, dbInterface = dbInterface) {
      override val errorSql =  s"SELECT col2, col1, col2 FROM $dummy_table"
      info(etTableContent.mkString("\n"))
    }
  }

}

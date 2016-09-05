package tech.artemisia.task.database.teradata

import tech.artemisia.core.AppLogger._
import tech.artemisia.task.database.DBInterface

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


}

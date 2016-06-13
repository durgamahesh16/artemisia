package tech.artemisia.task.database

import tech.artemisia.TestSpec

/**
 * Created by chlr on 4/27/16.
 */
class DBUtilSpec extends TestSpec {


  "DBUtil" must "parse table name with databasename" in {
    val tableName = "database.tablename"
    DBUtil.parseTableName(tableName) match {
      case (Some(db),table) => {
        db must be ("database")
        table must be ("tablename")
      }
      case _ => ???
    }
  }


}

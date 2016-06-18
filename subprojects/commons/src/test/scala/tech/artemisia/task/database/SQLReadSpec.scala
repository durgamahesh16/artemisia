package tech.artemisia.task.database

import tech.artemisia.TestSpec
import tech.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 4/28/16.
 */
class SQLReadSpec extends TestSpec {

  val table = "sql_read"
  val testDbInterface = TestDBInterFactory.withDefaultDataLoader(table)
  val connectionProfile = DBConnection("","","","",1000)

    "SQLRead" must "must a query and emit config object" in {
      val task = new SQLRead(sql = s"select col1,col2 from $table where col1 = 1"
        ,connectionProfile = connectionProfile) {
        override val dbInterface: DBInterface = testDbInterface
      }
      val result = task.execute
      result.getInt("COL1") must be (1)
      result.getString("COL2") must be ("foo")
    }


}

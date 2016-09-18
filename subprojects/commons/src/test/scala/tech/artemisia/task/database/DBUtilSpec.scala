package tech.artemisia.task.database

import tech.artemisia.TestSpec
import tech.artemisia.task.database.DBUtil.ResultSetIterator

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

  it must "iterator over an resultset as expected with data" in {
    val dBInterface = TestDBInterFactory.withDefaultDataLoader("db_utils")
    val rs = dBInterface.query("select * from db_utils")
   val data = new ResultSetIterator[(Int,String,Boolean,Short,Long,Float)](rs) {
      override def generateRow: (Int, String, Boolean, Short, Long, Float) = {
        (rs.getInt(1), rs.getString(2), rs.getBoolean(3), rs.getShort(4), rs.getLong(5), rs.getFloat(6))
      }
    }.toSeq
    data must contain theSameElementsInOrderAs Seq(
      (1,"foo",true,100: Short,10000000L,87.3F)
      ,(2,"bar",false,100: Short,10000000L,8723.38F)
    )
  }

  it must "iterator over an empty resultset and return an empty seq" in {

    val dBInterface = TestDBInterFactory.withDefaultDataLoader("db_utils")
    dBInterface.execute("DELETE FROM db_utils")
    val rs = dBInterface.query("select * from db_utils")
    val data = new ResultSetIterator[(Int,String,Boolean,Short,Long,Float)](rs) {
      override def generateRow: (Int, String, Boolean, Short, Long, Float) = {
        (rs.getInt(1), rs.getString(2), rs.getBoolean(3), rs.getShort(4), rs.getLong(5), rs.getFloat(6))
      }
    }.toSeq
    data mustBe empty
  }



}

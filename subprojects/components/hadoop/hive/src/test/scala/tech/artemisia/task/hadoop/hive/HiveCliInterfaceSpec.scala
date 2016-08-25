package tech.artemisia.task.hadoop.hive

import java.io.File
import org.apache.hadoop.io.IOUtils.NullOutputStream
import tech.artemisia.TestSpec
import tech.artemisia.util.TestUtils._
/**
  * Created by chlr on 8/7/16.
  */
class HiveCliInterfaceSpec extends TestSpec {

  "HiveCLIInterface" must "execute query and parse result for hive execute classic" in {
    runOnPosix {
      val task = new HQLExecute("hql_execute", "select * from table", None) {
        val file = new File(this.getClass.getResource("/executables/hive_execute_classic").getFile)
        file.setExecutable(true)
        override protected lazy val hiveCli = new HiveCLIInterface(file.toString, new NullOutputStream, new NullOutputStream)
      }
      val result = task.execute()
      result.getInt("hql_execute.__stats__.loaded.test_table") must be (52)
    }
  }

  it must "execute query and parse result for hive read classic" in {
    runOnPosix {
      val task = new HQLRead("hql_read", "select * from table", None) {
        val file = new File(this.getClass.getResource("/executables/hive_read").getFile)
        file.setExecutable(true)
        override protected lazy val hiveCli = new HiveCLIInterface(file.toString, new NullOutputStream, new NullOutputStream)
      }
      val result = task.execute()
      result.getInt("col1") must be (10)
      result.getString("col2") must be ("xyz")
    }
  }

  "HiveCLIInterface" must "execute query and parse result for hive execute yarn" in {
    runOnPosix {
      val task = new HQLExecute("hql_execute", "select * from table", None) {
        val file = new File(this.getClass.getResource("/executables/hive_execute_yarn").getFile)
        file.setExecutable(true)
        override protected lazy val hiveCli = new HiveCLIInterface(file.toString, new NullOutputStream, new NullOutputStream)
      }
      val result = task.execute()
      result.getInt("""hql_execute.__stats__.loaded."chlr_db.test_table"""") must be (1097)
    }
  }

}

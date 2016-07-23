package tech.artemisia.task.database.mysql

import java.io.File

import org.scalatest.BeforeAndAfterAll
import tech.artemisia.TestSpec
import tech.artemisia.task.database.TestDBInterFactory
import tech.artemisia.task.hadoop.{HDFSReadSetting, HDFSWriteSetting, TestHDFSCluster}
import tech.artemisia.task.settings.{BasicExportSetting, BasicLoadSetting, DBConnection}

/**
  * Created by chlr on 7/23/16.
  */
class HDFSTaskSpec extends TestSpec with BeforeAndAfterAll {

  import HDFSTaskSpec._

  override def beforeAll() = {
    cluster.initialize(this.getClass.getResource("/samplefiles").getFile)
  }

  "ExportToHDFS" must "export data to HDFS" in {
    val tableName = "mysqlExportHDFS"
    val task = new ExportToHDFS(tableName, s"SELECT * FROM $tableName",
      HDFSWriteSetting(cluster.pathToURI("/test/dir*/*.txt")), DBConnection("","","","",-1),
    BasicExportSetting()) {
      override val dbInterface =  TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("mysqlExportHDFS.__stats__.rows") must be (2)
  }

  "LoadFromHDFS" must "load data from HDFS" in {
    val tableName = "mysqlLoadFromHDFS"
    val task = new LoadFromHDFS(tableName, tableName, HDFSReadSetting(cluster.pathToURI("/test/file.txt"))
    , DBConnection("","","","",-1), BasicLoadSetting()) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt(s"$tableName.__stats__.loaded") must be (2)
    result.getInt(s"$tableName.__stats__.rejected") must be (0)
  }


  override def afterAll() = {
    cluster.terminate()
  }


}

object HDFSTaskSpec {

  val cluster = new TestHDFSCluster(new File(this.getClass.getResource("/hdfs").getFile))

}

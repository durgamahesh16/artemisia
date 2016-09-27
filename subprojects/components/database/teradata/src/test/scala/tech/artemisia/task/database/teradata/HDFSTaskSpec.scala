package tech.artemisia.task.database.teradata

import org.scalatest.BeforeAndAfterAll
import tech.artemisia.TestSpec
import tech.artemisia.task.database.{BasicExportSetting, TestDBInterFactory}
import tech.artemisia.task.hadoop.{HDFSReadSetting, HDFSUtil, HDFSWriteSetting, TestHDFSCluster}
import tech.artemisia.task.settings.DBConnection

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
    task.supportedModes must be === "default" :: "fastexport" :: Nil
    val result = task.execute()
    result.getInt("mysqlExportHDFS.__stats__.rows") must be (2)
  }

  "LoadFromHDFSHelper" must "load data from HDFS" in {
    val tableName = "mysqlLoadFromHDFS"
    val task = new LoadFromHDFS(tableName, tableName, HDFSReadSetting(cluster.pathToURI("/test/file.txt"))
      , DBConnection("","","","",-1), TeraLoadSetting()) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
      override lazy val source = Left(HDFSUtil.readIOStream(cluster.pathToURI("/test/file.txt")))
    }
    task.supportedModes must be === "default" :: "fastload" :: "auto" :: Nil
    val result = task.execute()
    result.getInt(s"$tableName.__stats__.loaded") must be (2)
    result.getInt(s"$tableName.__stats__.rejected") must be (0)
  }


  override def afterAll() = {
    cluster.terminate()
  }


}

object HDFSTaskSpec {

  val cluster = new TestHDFSCluster("teradata")

}

package tech.artemisia.task.hadoop

import java.io.{BufferedReader, InputStreamReader}
import org.scalatest.DoNotDiscover
import tech.artemisia.TestSpec
import tech.artemisia.task.database.{BasicExportSetting, BasicLoadSetting, DBInterface, TestDBInterFactory}
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 7/20/16.
  */

@DoNotDiscover
trait HDFSTaskSpec extends TestSpec {


  "ExportToHDFS" must "export data to HDFS" in {

    val tableName = "ExportToHDFSSpec_1"
    val location =  BaseHDFSSpec.cluster.pathToURI("/test/dir2/file100.txt")
     val task = new ExportToHDFS(
        "hdfs-task"
        ,s"SELECT * FROM $tableName"
        ,HDFSWriteSetting(location)
        ,DBConnection("hostname", "username", "password", "database", -1)
        ,BasicExportSetting()
      ) {
        override val supportedModes = "default" :: "bulk" :: Nil
        override val dbInterface: DBInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
      }
    task.execute()
    val stream = new BufferedReader(new InputStreamReader(HDFSUtil.readIOStream(location)))
    stream.readLine() must be ("1,foo,TRUE,100,10000000,87.30,12:30:00,1945-05-09,1945-05-09 12:30:00.0")
    stream.close()
  }

  "LoadFromHDFSHelper" must "load data from HDFS" in {
    val tableName = "LoadFromHDFSSpec_2"
    val location =  BaseHDFSSpec.cluster.pathToURI("/test/dir*/*.txt")
    val task = new LoadFromHDFS(
       "hdfs-task"
      ,tableName
      ,HDFSReadSetting(location)
      ,DBConnection("hostname", "username", "password", "db", -1)
      ,BasicLoadSetting(delimiter=',')
    ) {
      override val supportedModes = "default" :: "bulk" :: Nil
      override val dbInterface: DBInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("hdfs-task.__stats__.loaded") must be > 5
  }
}

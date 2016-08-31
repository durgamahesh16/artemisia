package tech.artemisia.task.hadoop

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.URI

import tech.artemisia.TestSpec
import tech.artemisia.task.database.{BasicLoadSetting, TestDBInterFactory}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.TestUtils._

/**
  * Created by chlr on 8/24/16.
  */
class HDFSCLIReaderSpec extends TestSpec {

  "HDFSCLIReader" must "read an HDFS file" in {
    runOnPosix {
      val binary = this.getClass.getResource("/executable/hdfs").getFile
      new File(binary).setExecutable(true)
      val cliReader = new HDFSCLIReader(binary)
      val buffer = new BufferedReader(new InputStreamReader(cliReader.readPath(new URI("hdfs://namenode/path"))))
      val result = Stream.continually(buffer.readLine()).takeWhile(x => x != null)
      result.head must be("100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00")
      result.length must be(4)
    }
  }

  it must "compute data volume in bytes" in {
    runOnPosix {
      val binary = this.getClass.getResource("/executable/hdfs").getFile
      new File(binary).setExecutable(true)
      val cliReader = new HDFSCLIReader(binary)
      val size = cliReader.getPathSize(new URI("hdfs://namenode/path"))
      size must be (1983515934)
    }
  }

  it must "work with LoadFromHDFS Task" in {
    runOnPosix {
      val tableName = "mysqlLoadFromHDFS"
      val binary = this.getClass.getResource("/executable/hdfs").getFile
      new File(binary).setExecutable(true)
      val task = new LoadFromHDFS(tableName, tableName, HDFSReadSetting(new URI("hdfs://namenode/path"))
        , DBConnection("", "", "", "", -1), BasicLoadSetting()) {
        override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
        override lazy val source = Left(new HDFSCLIReader(binary).readPath(hdfsReadSetting.location))
        override protected val supportedModes: Seq[String] = "default" :: Nil
      }
      val result = task.execute()
      result.getInt(s"$tableName.__stats__.loaded") must be(4)
      result.getInt(s"$tableName.__stats__.rejected") must be(0)
    }
  }

}

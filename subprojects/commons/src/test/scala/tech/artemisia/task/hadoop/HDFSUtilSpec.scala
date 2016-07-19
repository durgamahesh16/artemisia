package tech.artemisia.task.hadoop

import java.io._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.BeforeAndAfterAll
import tech.artemisia.TestSpec

/**
  * Created by chlr on 7/17/16.
  */
class HDFSUtilSpec extends TestSpec with BeforeAndAfterAll {

  val baseDir: File = new File(this.getClass.getClassLoader.getResource("arbitary/hdfs").toString)


  override def beforeAll: Unit = {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")
    System.setProperty("test.build.data",baseDir.toString)
    FileUtil.fullyDelete(baseDir)
    val conf = new Configuration()
    conf.set("dfs.datanode.data.dir", baseDir.toString)
    conf.set("dfs.namenode.logging.level","block")
    conf.setInt("dfs.block.size", 512)
    conf.setBoolean("dfs.support.broken.append", true)
    HDFSUtilSpec.dfs = new MiniDFSCluster(conf, 1, true, null)
    HDFSUtilSpec.dfs.waitActive()
    prepareHDFS()
  }

  "HDFSUtil" must "it must expand uri to file list" in {
   val uri = new URI(s"hdfs://localhost:${HDFSUtilSpec.dfs.getNameNodePort}/test/dir*/*.txt")
   val list = HDFSUtil.expandPath(uri, filesOnly = false)
    list must have length 4
  }

  it must "merge multiple files paths into a single stream" in {
    val uri = new URI(s"hdfs://localhost:${HDFSUtilSpec.dfs.getNameNodePort}/test/dir*/*.txt")
    val list = HDFSUtil.expandPath(uri, filesOnly = false)
    val stream = HDFSUtil.mergeFileIOStreams(list)
    val buffered = new BufferedReader(new InputStreamReader(stream))
    buffered.lines().toArray must have length 8
    buffered.close()
  }

  it must "read files in hdfs filesystem" in {
    val uri = new URI(s"hdfs://localhost:${HDFSUtilSpec.dfs.getNameNodePort}/test/dir1/file1.txt")
    val stream = new BufferedReader(new InputStreamReader(HDFSUtil.readIOStream(uri)))
    stream.readLine() must be ("100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00")
    stream.close()
  }

  it must "write files in hdfs filesystem" in {
    val data = "Hello World"
    val uri = new URI(s"hdfs://localhost:${HDFSUtilSpec.dfs.getNameNodePort}/test/dir1/file3.txt")
    val stream = HDFSUtil.writeIOStream(uri, true, 2048, 1, 2048)
    val buffered = new BufferedWriter(new OutputStreamWriter(stream))
    buffered.write(data)
    buffered.close()
    val reader = new BufferedReader(new InputStreamReader(HDFSUtil.readIOStream(uri)))
    reader.readLine() must be (data)
    reader.close()
  }


  it must "handle compression for both read and write" in {
    val data = "I find your lack of faith, disturbing"
    val uri = new URI(s"hdfs://localhost:${HDFSUtilSpec.dfs.getNameNodePort}/test/dir1/file4.gz")
    val writeStream = HDFSUtil.writeIOStream(
      uri =uri
    ,overwrite = true
    ,bufferSize = 2042
    ,replication = 1
    ,blockSize = 512
    ,codec = Some("bzip2"))
    val writer = new BufferedWriter(new OutputStreamWriter(writeStream))
    writer.write(data)
    writer.close()
    val readStream = HDFSUtil.readIOStream(uri, Some("bzip2"))
    val reader = new BufferedReader(new InputStreamReader(readStream))
    reader.readLine must be (data)
  }


  override def afterAll: Unit = {
    HDFSUtilSpec.dfs.shutdown()
    FileUtil.fullyDelete(baseDir)
  }


  private def prepareHDFS() = {
    val fileSystem: org.apache.hadoop.fs.FileSystem = HDFSUtilSpec.dfs.getFileSystem
    fileSystem.copyFromLocalFile(false, new Path(this.getClass.getClassLoader.getResource("arbitary/glob").toString),
      new Path("/test"))
  }


}

object HDFSUtilSpec {

  /**
    * since OneInstancePerTest trait is mixed in. dfs variable cannot be a instance member of the HDFSUtilSpec.
    * since each tests this class is run with its own instance.
    */
  var dfs: MiniDFSCluster = _

}

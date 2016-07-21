package tech.artemisia.task.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.BeforeAndAfterAll
import tech.artemisia.TestSpec

/**
  * Created by chlr on 7/21/16.
  */
class BaseHDFSSpec extends TestSpec with BeforeAndAfterAll with HDFSUtilSpec with HDFSTaskSpec {


  override def beforeAll: Unit = {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")
    System.setProperty("test.build.data",baseDir.toString)
    FileUtil.fullyDelete(baseDir)
    val conf = new Configuration()
    conf.set("dfs.datanode.data.dir", baseDir.toString)
    conf.set("dfs.namenode.logging.level","block")
    conf.setInt("dfs.block.size", 512)
    conf.setBoolean("dfs.support.broken.append", true)
    BaseHDFSSpec.dfs = new MiniDFSCluster(conf, 1, true, null)
    BaseHDFSSpec.dfs.waitActive()
    prepareHDFS()
  }


  private def prepareHDFS() = {
    val fileSystem: org.apache.hadoop.fs.FileSystem = BaseHDFSSpec.dfs.getFileSystem
    fileSystem.copyFromLocalFile(false, new Path(this.getClass.getClassLoader.getResource("arbitary/glob").toString),
      new Path("/test"))
  }



  override def afterAll: Unit = {
    BaseHDFSSpec.dfs.shutdown()
    FileUtil.fullyDelete(baseDir)
  }

}

object BaseHDFSSpec {

  /**
    * since OneInstancePerTest trait is mixed in. dfs variable cannot be a instance member of the HDFSUtilSpec.
    * since each tests this class is run with its own instance.
    */
  var dfs: MiniDFSCluster = _

}

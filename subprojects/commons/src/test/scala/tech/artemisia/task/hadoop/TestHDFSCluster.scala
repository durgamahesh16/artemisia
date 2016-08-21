package tech.artemisia.task.hadoop

import java.io.File
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.test.PathUtils

/**
  * Created by chlr on 7/22/16.
  */

class TestHDFSCluster(cluster: String) {

  var dfs: MiniDFSCluster = _
  val testDataPath = new File(PathUtils.getTestDir(this.getClass),cluster)
  var conf: HdfsConfiguration = _
  setup()

  private def setup() = {
    conf = new HdfsConfiguration()
    val testDataCluster1 = new File(testDataPath, cluster)
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataCluster1.getAbsolutePath)
    conf.setInt("dfs.blocksize", 512)
    conf.setInt("dfs.namenode.fs-limits.min-block-size", 512)
    dfs =  new MiniDFSCluster.Builder(conf).build()
    dfs.waitActive()
  }

  def initialize(sourceDir: String, hdfsDir: String = "/test") = {
    val fileSystem: org.apache.hadoop.fs.FileSystem = dfs.getFileSystem
    fileSystem.copyFromLocalFile(false, new Path(sourceDir),
      new Path(hdfsDir))
  }


  def pathToURI(path: String) = {
    new URI(s"hdfs://localhost:${dfs.getNameNodePort}$path")
  }

  def terminate() = {
    val dataDir = new Path(testDataPath.getParentFile.getParentFile.getParent)
    dfs.getFileSystem.delete(dataDir, true)
    val rootTestFile = new File(testDataPath.getParentFile.getParentFile.getParent)
    val rootTestDir = rootTestFile.getAbsolutePath
    val rootTestPath = new Path(rootTestDir)
    val localFileSystem = FileSystem.getLocal(conf)
    localFileSystem.delete(rootTestPath, true)
    dfs.shutdown()
  }



}

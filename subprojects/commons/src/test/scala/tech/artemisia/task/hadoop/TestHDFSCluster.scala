package tech.artemisia.task.hadoop

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster

/**
  * Created by chlr on 7/22/16.
  */

class TestHDFSCluster(baseDir: File) {

  var dfs: MiniDFSCluster = _
  setup()

  private def setup() = {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")
    System.setProperty("test.build.data",baseDir.toString)
    FileUtil.fullyDelete(baseDir)
    val conf = new Configuration()
    conf.set("dfs.datanode.data.dir", baseDir.toString)
    conf.set("dfs.namenode.logging.level","block")
    conf.setInt("dfs.block.size", 512)
    conf.setBoolean("dfs.support.broken.append", true)
    dfs = new MiniDFSCluster(conf, 1, true, null)
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
    dfs.shutdown()
    FileUtil.fullyDelete(baseDir)
  }



}

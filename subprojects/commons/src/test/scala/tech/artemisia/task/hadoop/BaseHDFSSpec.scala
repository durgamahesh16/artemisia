package tech.artemisia.task.hadoop

import java.io.File

import org.scalatest.BeforeAndAfterAll
import tech.artemisia.TestSpec

/**
  * Created by chlr on 7/21/16.
  */
class BaseHDFSSpec extends TestSpec with BeforeAndAfterAll with HDFSUtilSpec with HDFSTaskSpec {

  import BaseHDFSSpec._

  override def beforeAll: Unit = {
    cluster = new TestHDFSCluster(new File(this.getClass.getClassLoader.getResource("arbitary/hdfs").toString))
    cluster.initialize(this.getClass.getClassLoader.getResource("arbitary/glob").toString)
  }


  override def afterAll: Unit = {
    cluster.dfs.shutdown()
  }

}

object BaseHDFSSpec {

  /**
    * since OneInstancePerTest trait is mixed in. dfs variable cannot be a instance member of the HDFSUtilSpec.
    * since each tests this class is run with its own instance.
    */
  var cluster: TestHDFSCluster = _

}

package tech.artemisia.task.database.teradata

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.output.NullOutputStream
import tech.artemisia.TestSpec
import tech.artemisia.task.database.TestDBInterFactory
import tech.artemisia.task.database.teradata.tdch.{TDCHSetting, TDCHLogParser, TDCHLoad}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.HoconConfigUtil.Handler


/**
  * Created by chlr on 9/2/16.
  */
class TDCHLoadSpec extends TestSpec {

  "TDCHLoad" must "parse config and instantiate object" in {

    val tdchJar = this.getClass.getResource("/samplefiles/file.txt").getFile
    val hadoop = this.getClass.getResource("/executables/tdch_load_from_hdfs.sh").getFile
    val config = ConfigFactory parseString
        s"""
           |
           | {
           |      dsn = {
           |        host = teradata-server
           |        username = chlr
           |        password = password
           |        database = sandbox
           |        port = 1025
           |      }
           |    tdch-setting = {
           |      tdch-jar = $tdchJar
           |      hadoop = $hadoop
           |      queue-name = public
           |      format = textfile
           |      num-mappers = 10
           |      text-setting = {
           |         delimiter = ","
           |         quoting = no
           |         quote-char = "|"
           |         escape-char = "="
           |      }
           |      lib-jars = []
           |      misc-options = {}
           |    }
           |    source-type = hdfs
           |    source =  /user/chlr/load.txt
           |    target-table = database.tablename
           |    method = batch.insert
           |    truncate = yes
           |
           | }
         """.stripMargin
    val task = TDCHLoad("tdch_test", config).asInstanceOf[TDCHLoad]
    task.dBConnection.username must be ("chlr")
    task.truncate mustBe true
    task.method must be ("batch.insert")
    task.source must be ("/user/chlr/load.txt")
    task.targetTable must be ("database.tablename")
    task.tdchHadoopSetting.numMapper must be (10)
    task.tdchHadoopSetting.format must be ("textfile")

  }



  it must "execute as per the specification" in {
      val tdchJar = this.getClass.getResource("/samplefiles/file.txt").getFile
      val hadoop = new File(this.getClass.getResource("/executables/tdch_load_from_hdfs.sh").getFile)
      hadoop.setExecutable(true)
      val task = new TDCHLoad("testTDCHLoadFromHDFS", DBConnection.getDummyConnection, "hdfs", "/user/chlr/file.txt",
        "database.tablename", "batch.insert", false, TDCHSetting(tdchJar = tdchJar, hadoop = Some(hadoop))) {
         override val dbInterface = TestDBInterFactory.withDefaultDataLoader("tdchloadhdfs")
        override val logStream = new TDCHLogParser(new NullOutputStream)
      }
     val config = task.execute()
     config.as[Int]("testTDCHLoadFromHDFS.__stats__.rows") must be (100000)
  }

  it must "throw exception if incorrect source type is set" in {
    val tdchJar = this.getClass.getResource("/samplefiles/file.txt").getFile
    val hadoop = new File(this.getClass.getResource("/executables/tdch_load_from_hdfs.sh").getFile)
    hadoop.setExecutable(true)
    val exception = intercept[IllegalArgumentException] {
      new TDCHLoad("testTDCHLoadFromHDFS", DBConnection.getDummyConnection, "erroneous_source_type", "/user/chlr/file.txt",
        "database.tablename", "batch.insert", false, TDCHSetting(tdchJar = tdchJar, hadoop = Some(hadoop))) {
        override val dbInterface = TestDBInterFactory.withDefaultDataLoader("tdchloadhdfs")
        override val logStream = new TDCHLogParser(new NullOutputStream)
      }
    }
    exception.getMessage must be ("requirement failed: source-type erroneous_source_type is not supported. supported types are (hive,hdfs)")
  }

  it must "construct sourcetype related params" in {

    val args1 = TDCHLoad.generateSourceParams("/var/path/file.txt", "hdfs")
    args1("-jobtype") must be ("hdfs")
    args1("-sourcepaths") must be ("/var/path/file.txt")

    val args2 = TDCHLoad.generateSourceParams("database.table", "hive")
    args2("-jobtype") must be ("hive")
    args2("-sourcedatabase") must be ("database")
    args2("-sourcetable") must be ("table")

  }
}

package tech.artemisia.task.database.teradata

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.output.NullOutputStream
import tech.artemisia.TestSpec
import tech.artemisia.task.database.TestDBInterFactory
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.HoconConfigUtil.Handler


/**
  * Created by chlr on 9/2/16.
  */
class TDCHLoadFromHDFSSpec extends TestSpec {

  "TDCHLoadFromHDFS" must "parse config and instantiate object" in {

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
           |    source-path =  /user/chlr/load.txt
           |    target-table = database.tablename
           |    method = batch.insert
           |    truncate = yes
           |
           | }
         """.stripMargin
    val task = TDCHLoadFromHDFS("tdch_test", config).asInstanceOf[TDCHLoadFromHDFS]
    task.dBConnection.username must be ("chlr")
    task.truncate mustBe true
    task.method must be ("batch.insert")
    task.sourcePath must be ("/user/chlr/load.txt")
    task.targetTable must be ("database.tablename")
    task.tdchHadoopSetting.numMapper must be (10)
    task.tdchHadoopSetting.format must be ("textfile")

  }



  it must "execute as per the specification" in {
      val tdchJar = this.getClass.getResource("/samplefiles/file.txt").getFile
      val hadoop = new File(this.getClass.getResource("/executables/tdch_load_from_hdfs.sh").getFile)
      hadoop.setExecutable(true)
      val task = new TDCHLoadFromHDFS("testTDCHLoadFromHDFS", DBConnection.getDummyConnection, "/user/chlr/file.txt",
        "database.tablename", "batch.insert", false, TDCHSetting(tdchJar = tdchJar, hadoop = Some(hadoop))) {
         override val dbInterface = TestDBInterFactory.withDefaultDataLoader("tdchloadhdfs")
        override val logStream = new TDCHTDLoadLogParser(new NullOutputStream)
      }
     val config = task.execute()
     config.as[Int]("testTDCHLoadFromHDFS.__stats__.rows") must be (100000)
  }
}

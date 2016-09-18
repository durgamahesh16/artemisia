package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.output.NullOutputStream
import tech.artemisia.TestSpec
import tech.artemisia.task.database.TestDBInterFactory
import tech.artemisia.task.hadoop.HDFSReadSetting
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.TestUtils._

import scala.concurrent.Future

/**
  * Created by chlr on 9/16/16.
  */
class TPTLoadFromHDFSSpec extends TestSpec {

  "TPTLoadFromHDFS" must "construct itself from config" in {
    val config = ConfigFactory parseString
      """
        |{
        |   dsn = {
        |      host = server-name
        |      username = username
        |      password = password
        |      database = sandbox
        |      port = 1025
        |   }
        |   load = {
        |      delimiter = ","
        |      quoting = yes
        |      truncate = yes
        |      header = yes
        |      load-attrs = {}
        |      batch-size = 212312
        |      error-limit = 1234
        |      quotechar = "\""
        |      escapechar = "\\"
        |      mode = "fastload"
        |      skip-lines = 100
        |      dtconn-attrs = {
        |        MYDATACONNOPTR1 = Chicago
        |        MYDATACONNOPTR2 = {
        |          type = INTEGER
        |          value = Faizahville
        |        }
        |      }
        |      load-attrs = {
        |        MYLOADCONNOPTR1 = Gotham
        |        MYLOADCONNOPTR2 = {
        |         type = INTEGER
        |         value = Metropolis
        |        }
        |      }
        |   }
        |   destination-table = sandbox.chlr_test2
        |   hdfs = {
        |     location = /home/chlr/artemisia/confs/load.txt
        |     cli-mode = yes
        |     cli-binary = /usr/local/bin/hadoop
        |   }
        | }
      """.stripMargin
    val task = TPTLoadFromHDFS("test_job", config).asInstanceOf[TPTLoadFromHDFS]
    task.loadSetting.nullString must be (None)
    task.loadSetting.mode must be ("fastload")
    task.loadSetting.quotechar must be ('"')
    task.loadSetting.batchSize must be (212312)
    task.loadSetting.escapechar must be ('\\')
    task.loadSetting.delimiter must be (',')
    task.loadSetting.errorLimit must be (1234)
    task.loadSetting.dataConnectorAttrs must be (Map(
      "MYDATACONNOPTR1" -> ("VARCHAR","Chicago")
      ,"MYDATACONNOPTR2" -> ("INTEGER", "Faizahville")
    ))
    task.loadSetting.loadOperatorAttrs must be (Map(
      "MYLOADCONNOPTR1" -> ("VARCHAR", "Gotham"),
      "MYLOADCONNOPTR2" -> ("INTEGER", "Metropolis")
    )
    )
    task.connectionProfile.default_database must be ("sandbox")
    task.connectionProfile.hostname must be ("server-name")
    task.tableName must be ("sandbox.chlr_test2")
    task.location.toString must be ("/home/chlr/artemisia/confs/load.txt")
    task.hdfsReadSetting.location.toString must be ("/home/chlr/artemisia/confs/load.txt")
    task.hdfsReadSetting.cliBinary must be ("/usr/local/bin/hadoop")
  }


  it must "execute TPTLoadFromHDFS" in {
    val task = new TPTLoadFromHDFS("test_load"
      ,"test_table"
      ,HDFSReadSetting(this.getClass.getResource("/samplefiles/file.txt").toURI)
      ,DBConnection.getDummyConnection
      ,TPTLoadSetting()) {
      override implicit val dbInterface = TestDBInterFactory.withDefaultDataLoader("test_table")
      override lazy val tbuildBin = getExecutable(this.getClass.getResource("/executables/tbuild_load_from_file.sh"))
      override lazy val twbKillBin = getExecutable(this.getClass.getResource("/executables/nop_execute.sh"))
      override lazy val twbStat = getExecutable(this.getClass.getResource("/executables/nop_execute.sh"))
      override val logParser = new TPTLoadLogParser(new NullOutputStream())
      override lazy val readerFuture = Future.successful(())
      override val scriptGenerator = new TPTLoadOperScrGen(
        TPTLoadConfig("database", "table", "/var/path", "input.pipe"),
        TPTLoadSetting(dataConnectorAttrs = Map("ROWERRFILENAME" -> ("VARCHAR","/var/path/errorfile"))),
        DBConnection("td_server", "voltron", "password", "dbc", 1025)
      ) {
        override lazy val dbInterface = ???
        override lazy val tableMetadata = Seq(
          ("col1", "I1", 25: Short, "col1_1", "N"),
          ("col2", "I1", 25: Short, "col2_2", "Y")
        )
      }
    }
    val result = task.execute()
    result.getInt("test_load.__stats__.applied") must be (1000000)
    result.getInt("test_load.__stats__.sent") must be (1000000)
    result.getInt("test_load.__stats__.duplicate") must be (0)
  }

}

package tech.artemisia.task.database.teradata

import java.io.File
import com.typesafe.config.ConfigFactory
import tech.artemisia.util.HoconConfigUtil.Handler
import org.apache.commons.io.output.NullOutputStream
import tech.artemisia.TestSpec
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.FileSystemUtil

/**
  * Created by chlr on 9/4/16.
  */
class TDCHExtractSpec extends TestSpec {

  "TDCHExtract" must "throw exception if incorrect split-by is set" in {
    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val ex = intercept[IllegalArgumentException] {
      new TDCHExtract("tdchextract", DBConnection.getDummyConnection, "table", "database.table",
        "hdfs", "/hdfs/path", "unknown-split-type", false, TDCHSetting(file))
    }
     ex.getMessage must be ("requirement failed: split by unknown-split-type is not allowed. supported values are hash,partition,amp,value")
  }


  it must "throw exception if unknown source type is set" in {
    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val ex = intercept[IllegalArgumentException] {
      new TDCHExtract("tdchextract", DBConnection.getDummyConnection, "unknown-source", "database.table",
        "hdfs", "/hdfs/path", "hash", false, TDCHSetting(file))
    }
   ex.getMessage must be ("requirement failed: source-type unknown-source is not allowed. supported values are table,query")
  }

  it must "throw exception if unknown target type is set" in {

    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val ex = intercept[IllegalArgumentException] {
      new TDCHExtract("tdchextract", DBConnection.getDummyConnection, "table", "database.table",
        "unknown-target", "/hdfs/path", "hash", false, TDCHSetting(file))
    }
    ex.getMessage must be ("requirement failed: target-type unknown-target is not allowed. supported values are hdfs,hive")
  }


  it must "extract data when executed" in {

    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val binary = new File(this.getClass.getResource("/executables/tdch_extract_to_hdfs.sh").getFile)
    binary.setExecutable(true)
     val task = new TDCHExtract("tdchextract", DBConnection.getDummyConnection, "table", "database.table",
        "hdfs", "/hdfs/path", "hash", false, TDCHSetting(file, hadoop = Some(binary))) {
        override val logStream = new TDCHLogParser(new NullOutputStream)
     }
    val config = task.execute()
    config.as[Int]("tdchextract.__stats__.rows") must be (100000)
  }

  it must "contruct object from config file" in {
    val tdchJar = this.getClass.getResource("/samplefiles/file.txt").getFile
    val hadoop = this.getClass.getResource("/executables/tdch_load_from_hdfs.sh").getFile
    val dir1 = this.getClass.getResource("/samplefiles/dir3").getFile
    val dir2 = FileSystemUtil.joinPath(this.getClass.getResource("/samplefiles/dir2").getFile, "file*.txt")
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
         |      lib-jars = ["$dir1","$dir2"]
         |      misc-options = {}
         |    }
         |    source-type = table
         |    source = database.tablename
         |    target-type =  hdfs
         |    target = /path/hdfs
         |    split-by = hash
         |    truncate = yes
         | }
         """.stripMargin
    val task = TDCHExtract("tdch_test", config).asInstanceOf[TDCHExtract]
    task.dBConnection.password must be ("password")
    task.truncate mustBe true
    task.target must be ("/path/hdfs")
    task.targetType must be ("hdfs")
    task.source must be ("database.tablename")
    task.sourceType must be ("table")
    task.tdchHadoopSetting.libJars must have size 3
    task.splitBy must be ("hash")
  }




}

package tech.artemisia.task.database.teradata.tdch

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec
import tech.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 9/1/16.
  */
class TDCHSettingSpec extends TestSpec {

  "TDCHSettingSpec" must "throw exception if tdchjar doesn't exist" in {
    val ex = intercept[java.lang.IllegalArgumentException] {
      TDCHSetting("/path/jar_file_that_doesn't_exist.jar")
    }
    ex.getMessage must be ("requirement failed: TDCH jar /path/jar_file_that_doesn't_exist.jar doesn't exists")
  }

  it must "throw exception when unsupported format is specified" in {
    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val ex = intercept[java.lang.IllegalArgumentException] {
      TDCHSetting(file, format = "some-unsupported-format")
    }
    ex.getMessage must be ("requirement failed: some-unsupported-format is not supported. " +
      "textfile,avrofile,rcfile,orcfile,sequenceFile are the only format supported")
  }


  it must "throw an exception when the hadoop binary doesn't exists" in {
    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val ex = intercept[java.lang.IllegalArgumentException] {
      TDCHSetting(file, hadoop = Some(new File("non-existant-hadoop")))
    }
    ex.getMessage must be ("requirement failed: hadoop binary non-existant-hadoop doesn't exists")
  }


  it must "construct the object properly" in {
    val tdchJar = this.getClass.getResource("/samplefiles/file.txt").getFile
    val hadoop = this.getClass.getResource("/executables/tdch_load_from_hdfs.sh").getFile
    val dir = this.getClass.getResource("/samplefiles").getFile
    val config = ConfigFactory parseString
    s"""
      |  {
      |   tdch-jar = $tdchJar
      |   hadoop = $hadoop
      |   num-mappers = 5
      |   queue-name = test
      |   format = textfile
      |   lib-jars = [
      |     ${joinPath(dir, "file.txt")}
      |     "${joinPath(dir, "dir2" , "file*.txt")}"
      |   ]
      |   misc-options = {
      |     foo1 = bar1
      |     foo2 = bar2
      |   }
      |   text-setting = {
      |    delimiter = ","
      |    quoting = no
      |    quote-char = "'"
      |    escape-char = "|"
      |    null-string = ""
      |   }
      | }
    """.stripMargin
    val setting = TDCHSetting(config)

    setting.tdchJar must be (tdchJar)
    setting.hadoop.get.toString must be (hadoop)
    setting.numMapper must be (5)
    setting.queueName must be ("test")
    setting.format must be ("textfile")
    (setting.libJars map { x => Paths.get(x).getFileName.toString }) must contain inOrderOnly ("file.txt", "file1.txt", "file2.txt")
    setting.miscOptions must contain allOf ("foo1" -> "bar1", "foo2" -> "bar2")
    setting.textSettings.delimiter must be (',')
    setting.textSettings.quoting mustBe false
    setting.textSettings.quoteChar must be ('\'')
    setting.textSettings.escapedBy must be ('|')
    setting.textSettings.nullString.get must be ("")
  }

}

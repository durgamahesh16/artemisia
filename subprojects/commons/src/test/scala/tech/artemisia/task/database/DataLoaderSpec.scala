package tech.artemisia.task.database

import java.io.FileInputStream
import java.nio.file.Paths

import tech.artemisia.TestSpec
import tech.artemisia.task.TaskContext
import tech.artemisia.util.FileSystemUtil
import tech.artemisia.util.FileSystemUtil.{FileEnhancer, withTempFile}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.io.Source


/**
 * Created by chlr on 5/13/16.
 */
class DataLoaderSpec extends TestSpec {


  "DataLoader" must "properly load a file" in {
    val tableName = "DataLoaderSpec_1"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    dbInterface.execute(s"delete from $tableName")
    withTempFile(fileName = "DataLoaderSpec1") {
        file => {
          val loadSettings = BasicLoadSetting(batchSize = 1)
          file <<=
            """ |100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
                |101,bravo,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
                |102,whiskey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
                |103a,blimey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
                |104,victor,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00 """.stripMargin
          val (recordCnt, rejectedCnt) = dbInterface.loadTable(tableName, Left(new FileInputStream(file)),loadSettings)
          val config = dbInterface.queryOne(s"SELECT COUNT(*) as cnt FROM $tableName")
          config.as[Int]("CNT") must be (4)
          recordCnt must be (5)
          rejectedCnt must be (1)
      }
    }
  }


  it must "presists invalid records in reject file" in {
    val tableName = "DataLoaderSpec_2"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    dbInterface.execute(s"delete from $tableName")
    withTempFile(fileName = "DataLoaderSpec2") {
      file => {
        val loadSettings = BasicLoadSetting(delimiter = ',', batchSize = 1)
        file <<=
          """|100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |101,bravo,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |102z,whiskey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |104,victor,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00""".stripMargin
        dbInterface.loadTable(tableName, Left(new FileInputStream(file)) ,loadSettings)
        val config = dbInterface.queryOne(s"SELECT COUNT(*) as cnt FROM $tableName")
        config.as[Int]("CNT") must be (3)
        Source.fromFile(TaskContext.getTaskFile("error.txt")).getLines().mkString("\n") must be (
          "102z\u0001whiskey\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00"
        )
      }
    }
  }


  it must "fail when rejection percentage is greater than allowed limit" in {
    val tableName = "DataLoaderSpec_3"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    dbInterface.execute(s"delete from $tableName")
    withTempFile(fileName = "DataLoaderSpec3") {
      file => {
        val loadSettings = BasicLoadSetting(delimiter = ',', errorTolerance = Some(0.5))
        file <<=
          """|100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |101,bravo,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |102z,whiskey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |103,victor,100,10000000,12:30:00,1945-05-09,1945-05-09 12:30:00
             |104d,november""".stripMargin
        val ex = intercept[AssertionError]{
          dbInterface.loadTable(tableName, Left(new FileInputStream(file)) ,loadSettings)
        }
        ex.getMessage must be ("assertion failed: Load Error % 60.00 greater than defined limit: 50.0")
      }
    }
  }


  it must "handle blank string as null" in {
    val tableName = "DataLoaderSpec_4"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    dbInterface.execute(s"delete from $tableName")
    withTempFile(fileName = tableName) {
      file => {
        val loadSettings = BasicLoadSetting(delimiter = ',')
        file <<=
          """|100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |101,bravo,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |102,,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
             |,victor,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00""".stripMargin
          dbInterface.loadTable(tableName, Left(new FileInputStream(file)) ,loadSettings)
        var result = dbInterface.queryOne(s"SELECT col1 FROM $tableName WHERE col2 IS NULL")
        result.as[Int]("COL1") must be (102)
        result = dbInterface.queryOne(s"SELECT col2 FROM $tableName WHERE col1 IS NULL")
        result.as[String]("COL2") must be ("victor")
      }
    }
  }


  it must "load mutiple files via globbed path" in {
    val tableName = "DataLoaderSpec_5"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    val path = this.getClass.getClassLoader.getResource("arbitary/glob")
    val location = FileSystemUtil.joinPath(path.getFile ,"**/*.txt")
    val stream = FileSystemUtil.mergeFileStreams(FileSystemUtil.expandPathToFiles(Paths.get(location)))
    dbInterface.execute(s"delete from $tableName")
    val loadSettings = BasicLoadSetting(delimiter = ',')
    val result = dbInterface.loadTable(tableName, Left(stream) ,loadSettings)
    result._1 must be (8)
  }


}

package tech.artemisia.task.hadoop

import java.io.{ByteArrayOutputStream, InputStream, PipedInputStream, PipedOutputStream}
import java.net.URI
import tech.artemisia.core.AppLogger._
import tech.artemisia.util.CommandUtil._
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by chlr on 8/21/16.
  */

/**
  * A HDFS CLI Reader that uses the locally installed Hadoop shell utilities
  * to read from HDFS.
  *
  * @param cli
  */
class HDFSCLIReader(cli: String) {

  /**
    *
    * @param path hdfs path to be read
    * @return inputstream for the data to read
    */
  def readPath(path: String): InputStream = {
    val command = cli :: "dfs" :: "-text" :: path :: Nil
    val inputStream = new PipedInputStream()
    val outputStream = new PipedOutputStream(inputStream)
    Future {
      executeCmd(command, outputStream)
    } onComplete  {
      case Success(retCode) =>
        debug("reading from path $path completed successfully")
        outputStream.close()
        assert(retCode == 0, s"command ${command.mkString(" ")} failed with retcode $retCode")
      case Failure(th) =>
        outputStream.close()
        throw th;
    }
    inputStream
  }

  /**
    *
    * @param path hdfs path to be read
    * @return inputstream for the data to read
    */
  def readPath(path: URI): InputStream = {
    readPath(path.toString)
  }


  /**
    * get the total volume of data a given HDFS path holds.
    * @param path HDFS path to inspect
    * @return total size in bytes
    */
  def getPathSize(path: URI): Long = {
    getPathSize(path.toString)
  }


  /**
    * get the total volume of data a given HDFS path holds.
    * @param path HDFS path to inspect
    * @return total size in bytes
    */
  def getPathSize(path: String): Long = {
    val command = cli :: "dfs" :: "-du" :: path :: Nil
    val cmdResult = new ByteArrayOutputStream()
    val result = executeCmd(command, cmdResult)
    assert(result == 0, s"command ${command.mkString(" ")} failed with return code $result")
    val output = new String(cmdResult.toByteArray)
    val num = output.split(System.lineSeparator()).filterNot(_.startsWith("Found")) // tail is done to remove the first line which says 'Found n items'
      .map(_.split("\\s+").head.toLong).sum
    debug(s"the total size of path $path is $num bytes")
    num
  }

}

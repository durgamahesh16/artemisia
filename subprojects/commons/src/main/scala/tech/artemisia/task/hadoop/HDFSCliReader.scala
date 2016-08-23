package tech.artemisia.task.hadoop

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URI

import com.Ostermiller.util.CircularByteBuffer
import tech.artemisia.core.AppLogger._
import tech.artemisia.util.CommandUtil._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

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
    val buffer = new CircularByteBuffer(10485760)
    Future {
      executeCmd(command, buffer.getOutputStream)
    } onComplete  {
      case Success(retCode) =>
        debug("reading from path $path completed successfully")
        buffer.getOutputStream.close()
        assert(retCode == 0, s"command ${command.mkString(" ")} failed with retcode $retCode")
      case Failure(th) =>
        buffer.getOutputStream.close()
        throw th;
    }
    buffer.getInputStream
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

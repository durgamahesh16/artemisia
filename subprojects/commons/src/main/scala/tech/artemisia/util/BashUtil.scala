package tech.artemisia.util

import java.io.ByteArrayOutputStream
import CommandUtil._
import tech.artemisia.core.AppLogger._

/**
  * This class hosts utility methods that is guaranteed to work only in Bash shell.
  * It might or might not work in other Linux shells.
  */
object BashUtil {


  /**
    * expand path to a sequence of files
    * @param path
    */
  def expandPath(path: String): Seq[String] = {
    val byteStream = new ByteArrayOutputStream()
    executeShellCommand(s"ls -1 $path", stdout = byteStream)
    byteStream.toString.split(System.lineSeparator)
  }


  /**
    * return total size of the path in bytes.
    * @param path input path
    * @return
    */
  def pathSize(path: String): Long = {
    val byteStream = new ByteArrayOutputStream()
    executeShellCommand(s"du $path", stdout = byteStream)
    val size = byteStream.toString.split(System.lineSeparator).map(_.split("[\\s]+").head.toLong).sum
    debug(s"total size of $path is $size")
    size
  }
}

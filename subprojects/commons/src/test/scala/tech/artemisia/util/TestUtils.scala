package tech.artemisia.util

import java.io.File
import java.net.URL
import java.nio.file.FileSystems

/**
  * Created by chlr on 8/7/16.
  */
object TestUtils {

  val isPosixOS = FileSystems.getDefault().supportedFileAttributeViews().contains("posix")

  /**
    * run test code block only if the OS is a POSIX complaint system.
    * @param body
    */
  def runOnPosix(body: => Unit) {
    if(isPosixOS) {
      body
    } else {
      System.err.println(s"skipping test to be run only on posix system")
    }
  }

  /**
    * get test executable file path
    * @param file
    * @return
    */
  def getExecutable(file: URL) = {
    val handle = new File(file.getFile)
    handle.setExecutable(true)
    handle.toString
  }

}

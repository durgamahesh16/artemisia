package tech.artemisia.util

import java.nio.file.FileSystems

/**
  * Created by chlr on 8/7/16.
  */
object TestUtils {

  val isPosixOS = FileSystems.getDefault().supportedFileAttributeViews().contains("posix")

  def runOnPosix(body: => Unit) {
    if(isPosixOS) {
      body
    } else {
      System.err.println(s"skipping test to be run only on posix system")
    }
  }

}

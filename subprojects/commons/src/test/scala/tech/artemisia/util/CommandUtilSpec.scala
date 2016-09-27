package tech.artemisia.util

import java.io.{ByteArrayOutputStream, File}
import CommandUtil._
import tech.artemisia.TestSpec

/**
  * Created by chlr on 8/7/16.
  */

class CommandUtilSpec extends TestSpec {


  "CommandUtilSpec" must "get executable from path" in {
    val randomFileInPath = new File(System.getenv("PATH").split(File.pathSeparator).head).list.head
    CommandUtil.getExecutablePath(randomFileInPath) match {
      case Some(x) => x.split(File.separatorChar).last must be (randomFileInPath)
      case None => fail(s"$randomFileInPath was not found in path")
    }
  }

  it must "must executed command" in {
    TestUtils.runOnPosix {
      val cmd: Seq[String] = "echo" :: "hello" :: "world" :: Nil
      val stream = new ByteArrayOutputStream()
      val result = CommandUtil.executeCmd(cmd, stdout = stream)
      result must be (0)
      new String(stream.toByteArray).trim must be ("hello world")
    }
  }

  it must "obsfucate commands when needed" in {
    val command = "binary" :: "-password" :: "bingo" :: Nil
    CommandUtil.obfuscatedCommandString(command, Seq(3)) must be ("binary -password *****")
  }

  it must "ignore non-zero return codes when requested" in {
    val path = TestUtils.getExecutable(this.getClass.getResource("/executable/script_that_fails.sh"))
    val command = Seq(path, "3")
    executeCmd(command, validExitValues = Array(3))
    val ex = intercept[org.apache.commons.exec.ExecuteException] {
      executeCmd(command)
    }
    ex.getMessage must be ("Process exited with an error: 3 (Exit value: 3)")
  }


}

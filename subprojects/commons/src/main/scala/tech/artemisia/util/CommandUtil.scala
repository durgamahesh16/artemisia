package tech.artemisia.util

import java.io.{File, OutputStream}
import org.apache.commons.exec.{CommandLine, DefaultExecutor, PumpStreamHandler}
import scala.collection.JavaConverters._
import tech.artemisia.core.AppLogger._

/**
  * Created by chlr on 8/3/16.
  */

object CommandUtil {


  /**
    *
    * Execute commands
    * @param command arguments to the executable
    * @param stdout standard output stream
    * @param stderr standard error stream
    * @param env environment variables to be used in addition to existing variables
    * @param cwd current working directory
    * @param obfuscate this specific argument while logging the command esp passwords.
    * @return return code of the command
    */
  def executeCmd(command: Seq[String], stdout: OutputStream = System.out, stderr: OutputStream = System.err
                 ,env: Map[String, String] = Map(), cwd: Option[File] = None, obfuscate: Seq[Int] = Nil): Int = {
    val cmdLine = new CommandLine(command.head)
    command.tail foreach cmdLine.addArgument
    val executor = new DefaultExecutor()
    cwd foreach executor.setWorkingDirectory
    executor.setStreamHandler(new PumpStreamHandler(stdout, stderr))
    debug(s"""executing command ${obfuscatedCommandString(command, obfuscate)}""")
    executor.execute(cmdLine, (env ++ System.getenv().asScala).asJava)
  }


  /**
    * obfuscate sections of command string
    * @param command command sequence string
    * @param sections parts of the command sequence string to obfuscate
    * @return
    */
  def obfuscatedCommandString(command: Seq[String], sections: Seq[Int]) = {
    val boolList = for (i <- 1 to command.length) yield { sections contains i }
    command zip boolList map {
      case (x, true) => "*" * 5
      case (x, false) => x
    } mkString " "
  }

  /**
    * get the path of the executable by searching in the path environment variable
    *
    * @param executable name of the executable
    * @return
    */
  def getExecutablePath(executable: String) = {
    val separator = System.getProperty("path.separator")
    scala.util.Properties.envOrNone("PATH") match {
      case Some(x) =>  x.split(separator)
              .find(new File(_).listFiles().map(_.getName).contains(executable))
              .map(new File(_, executable).toPath.toString)
      case None => None
    }
  }


  /**
    * search for an executable in PATH and if not found throw an exception.
    * @param executable executable to search for
    * @return absolute path of executable
    */
  def getExecutableOrFail(executable: String) = {
    getExecutablePath(executable) match {
      case Some(exe) => exe
      case None => throw new RuntimeException(s"$executable not found in PATH")
    }
  }

}

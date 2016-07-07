package tech.artemisia.task.localhost

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.{AppLogger, Keywords}
import tech.artemisia.task.localhost.util.ProcessRunner
import tech.artemisia.task.{Task, TaskContext, TaskLike}
import tech.artemisia.util.FileSystemUtil.{FileEnhancer, withTempFile}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.Util

/**
 * Created by chlr on 2/21/16.
 */
class ScriptTask(name: String = Util.getUUID, script: String,interpreter: String = "/bin/sh" ,cwd: String = Paths.get("").toAbsolutePath.toString
                 , env: Map[String,String] = Map()
                 , parseOutput: Boolean = false) extends Task(name: String) {


  val processRunner : ProcessRunner = new ProcessRunner(interpreter)
  val scriptFileName = "script.sh"

  override def setup(): Unit = {
    this.writeToFile(script,scriptFileName)
  }

  override def work(): Config = {
    AppLogger info s"executing script"
    AppLogger info Util.prettyPrintAsciiBanner(script, heading = "script")
    var result: (String, String, Int) = null
    withTempFile(TaskContext.workingDir.toString,this.getFileHandle(scriptFileName).toString) {
      file => {
        file <<= script
         result = processRunner.executeFile(cwd,env) {
          file.toPath.toAbsolutePath.toString
        }
      }
    }

    AppLogger debug s"stdout detected: ${result._1}"
    AppLogger debug s"stderr detected: ${result._2}"

    assert(result._3 == 0, "Non Zero return code detected")
    ConfigFactory parseString { if (parseOutput) result._1 else "" }
  }

  override def teardown(): Unit = {}

}

object ScriptTask extends TaskLike {

  override val taskName = "ScriptTask"

  override val info = "executes script with customizable interpreter"

  val defaultConfig = ConfigFactory parseString
    s"""
      | {
      |   interpreter = "/bin/sh"
      |   cwd = ${Paths.get("").toAbsolutePath.toString}
      |   parse-output = no
      |   env = {}
      | }
    """.stripMargin

  override def apply(name: String, inputConfig: Config) = {
    val config = inputConfig withFallback defaultConfig
    new ScriptTask (
      name
     ,script = config.as[String]("script")
     ,interpreter = config.as[String]("interpreter")
     ,cwd = config.as[String]("cwd")
     ,env = config.asMap[String]("env")
     ,parseOutput = config.as[Boolean]("parse-output")
    )
  }

  override val desc: String = ""

  override def configStructure(component: String): String = {
    s"""
       | ${Keywords.Task.COMPONENT} = $component
       | ${Keywords.Task.COMPONENT} = $taskName
       | ${Keywords.Task.PARAMS} = {
       |   script = "echo Hello World" @required
       |   interpreter = "/usr/local/bin/sh" @default("/bin/sh")
       |   cwd = "/var/tmp" @default("<your current working directory>")
       |   env = { foo = bar, hello = world } @default("<empty object>")
       |   parse-output = yes @default(false)
       | }
     """.stripMargin
  }

  override val fieldDefinition = Seq(
    "script" -> "string whose content while be flushed to a temp file and executed with the interpreter",
    "interpreter" -> "the interpreter used to execute the script. it can be bash, python, perl etc",
    "cwd" -> "set the current working directory for the script execution",
    "env" -> "environmental variables to be used",
    "parse-output" -> "parse the stdout of script which has to be a Hocon config (Json superset) and merge the result to the job config"
  )
}

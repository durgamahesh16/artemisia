package org.ultron.task.localhost


import java.nio.file.Paths
import com.typesafe.config.{Config, ConfigFactory}
import org.ultron.core.AppLogger
import org.ultron.task.localhost.util.ProcessRunner
import org.ultron.task.Task
import org.ultron.util.Util
import org.ultron.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 2/21/16.
 */
class ScriptTask(name: String = Util.getUUID, script: String,interpreter: String = ScriptTask.Defaults.interpreter ,cwd: String = ScriptTask.Defaults.cwd
                 , env: Map[String,String] = ScriptTask.Defaults.env
                 , parseOutput: Boolean = ScriptTask.Defaults.parseOutput) extends Task(name: String) {


  val processRunner : ProcessRunner = new ProcessRunner(interpreter)
  val scriptFileName = "script.sh"

  override def setup(): Unit = {
    this.writeToFile(script,scriptFileName)
  }

  override def work(): Config = {
    AppLogger info s"executing script"
    AppLogger info Util.prettyPrintAsciiTable(script, heading = "script")
    val result = processRunner.executeFile(cwd,env) {
      this.getFileHandle(scriptFileName).toPath.toAbsolutePath.toString
    }
    if (result._1.length > 0)
      AppLogger debug s"stdout detected: ${result._1}"
    if (result._2.length > 0)
      AppLogger debug s"stderr detected: ${result._2}"
    assert(result._3 == 0, "Non Zero return code detected")
      ConfigFactory parseString {
        if (parseOutput) result._1 else ""
      }
  }

  override def teardown(): Unit = {}

}

object ScriptTask {

  object Defaults {
    val cwd = Paths.get("").toAbsolutePath.toString
    val env = Map[String,String]()
    val parseOutput = false
    val interpreter = "/bin/sh"
  }

  def apply(name: String,config: Config) = {
    new ScriptTask (
      name
     ,script = config.as[String]("script")
     ,interpreter = if (config.hasPath("cwd")) config.as[String]("interpreter") else ScriptTask.Defaults.interpreter
     ,cwd = if (config.hasPath("cwd")) config.as[String]("cwd") else ScriptTask.Defaults.cwd
     ,env = if (config.hasPath("env")) config.asMap[String]("env") else ScriptTask.Defaults.env
     ,parseOutput = if (config.hasPath("parse_output")) config.as[Boolean]("env") else ScriptTask.Defaults.parseOutput
    )
  }

}

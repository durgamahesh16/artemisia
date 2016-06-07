package tech.artemisia.core

import java.nio.file.Paths

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import tech.artemisia.dag.{ActorSysManager, Dag}
import tech.artemisia.task.TaskContext
import tech.artemisia.util.HoconConfigUtil.Handler

object Runner {


  def run(appContext: AppContext) = {
    prepare(appContext)
    val dag = Dag(appContext)
    AppLogger debug "starting Actor System"
    val actor_sys_manager =  new ActorSysManager(appContext)
    val workers = actor_sys_manager.createWorker(Keywords.ActorSys.CUSTOM_DISPATCHER)
    val dag_player = actor_sys_manager.createPlayer(dag,workers)
    dag_player ! 'Play
  }

  private def prepare(appContext: AppContext) = {
    configureLogging(appContext)
    AppLogger debug s"workflow_id: ${appContext.runId}"
    AppLogger debug s"working directory: ${appContext.workingDir}"
    if (appContext.globalConfigFile.nonEmpty) {
      AppLogger debug s"global config file: ${appContext.globalConfigFile.get}"
    }
    TaskContext.setWorkingDir(Paths.get(appContext.workingDir))
    TaskContext.predefinedConnectionProfiles = TaskContext.parseConnections(
      appContext.payload.as[Config](Keywords.Config.CONNECTION_SECTION))
  }


  private def configureLogging(app_context: AppContext) = {
    val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val jc = new JoranConfigurator
    jc.setContext(context)
    context.reset()
    context.putProperty("log.console.level", app_context.logging.console_trace_level)
    context.putProperty("log.file.level", app_context.logging.file_trace_level)
    context.putProperty("env.working_dir", app_context.workingDir)
    context.putProperty("workflow_id", app_context.runId)
    jc.doConfigure(this.getClass.getResourceAsStream("/logback_config.xml"))
  }

}

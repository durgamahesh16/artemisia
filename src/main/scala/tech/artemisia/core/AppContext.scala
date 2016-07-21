package tech.artemisia.core

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import tech.artemisia.core.AppContext.{DagSetting, Logging}
import tech.artemisia.core.BasicCheckpointManager.CheckpointData
import tech.artemisia.dag.Message.TaskStats
import tech.artemisia.task.{Component, TaskContext}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.{FileSystemUtil, Util}

import scala.concurrent.duration.FiniteDuration


/**
 *  Created by chlr on 11/28/15.
 */


class AppContext(private val cmdLineParam: AppSetting) {

  val skipCheckpoints = cmdLineParam.skip_checkpoints
  val globalConfigFile = cmdLineParam.globalConfigFileRef
  var payload = getConfigObject
  val logging: Logging =  AppContext.parseLoggingFromPayload(payload.as[Config](s"${Keywords.Config.SETTINGS_SECTION}.logging"))
  val dagSetting: DagSetting = AppContext.parseDagSettingFromPayload(payload.as[Config](s"${Keywords.Config.SETTINGS_SECTION}.dag"))
  val runId: String = cmdLineParam.run_id.getOrElse(Util.getUUID)
  val workingDir: String = computeWorkingDir
  private val checkpointMgr = if (skipCheckpoints) new BasicCheckpointManager else new FileCheckPointManager(checkpointFile)
  val componentMapper: Map[String,Component] = payload.asMap[String](s"${Keywords.Config.SETTINGS_SECTION}.components") map {
    case (name,component) => { (name, Class.forName(component).getConstructor(classOf[String]).newInstance(name).asInstanceOf[Component] ) }
  }

  TaskContext.setWorkingDir(Paths.get(this.workingDir))

  /**
   * merge all config objects (Global, Code, Context) to provide unified code config object
   * @return full unified config object
   */
  private[core] def getConfigObject: Config = {
    val empty_object = ConfigFactory.empty()
    val reference = ConfigFactory parseFile new File(System.getProperty(Keywords.Config.SYSTEM_DEFAULT_CONFIG_FILE_JVM_PARAM))
    val context = (cmdLineParam.context map ( ConfigFactory parseString _ )).getOrElse(empty_object)
    val config_file = (cmdLineParam.config map { x => Util.readConfigFile(new File(x)) }).getOrElse(empty_object)
    val code = (cmdLineParam.cmd filter { _ == "run" } map
      { x => Util.readConfigFile(new File(cmdLineParam.value.get)) }).getOrElse(empty_object)
    val global_config_option = (globalConfigFile map { x => Util.readConfigFile(new File(x)) } ).getOrElse(empty_object)
    context withFallback config_file withFallback code withFallback global_config_option withFallback reference
  }

  override def toString = {
    val options = ConfigRenderOptions.defaults() setComments false setFormatted true setOriginComments false setJson true
    payload.root().render(options)
  }

  /**
   * @return checkpoint file for the session
   */
  def checkpointFile = new File(FileSystemUtil.joinPath(workingDir,Keywords.Config.CHECKPOINT_FILE))

  /**
   *
   * @param taskName
   * @param taskStats
   */
  def commitCheckpoint(taskName: String, taskStats: TaskStats) = {
    checkpointMgr.save(taskName, taskStats)
    payload = checkpointMgr.checkpoints.adhocPayload withFallback payload
  }

  /**
   * 
   * @return checkpoint data encapsulated in CheckPointData object
   */
  def checkpoints: CheckpointData = {
    checkpointMgr.checkpoints
  }


  /**
   * compute the effective working directory. The directory selection has the following precedence
   *  a) the one assigned via command line param
   *  b) the one defined in the settings node of the payload
   *  c) deterministic folder created in the temp directory of the system
   * @return working dir selected for the job
   */
  private[core] def computeWorkingDir = {
    val configAssigned  = payload.getAs[String]("__settings__.core.working_dir") map { FileSystemUtil.joinPath(_,runId) }
    val cmdLineAssigned = cmdLineParam.working_dir
    val defaultAssigned = FileSystemUtil.joinPath(FileSystemUtil.baseDir.toString,runId)
    cmdLineAssigned.getOrElse(configAssigned.getOrElse(defaultAssigned))
  }

}


object AppContext {

  private[core] case class DagSetting(attempts: Int, concurrency: Int, heartbeat_cycle: FiniteDuration,
                                        cooldown: FiniteDuration, disable_assertions: Boolean, ignore_conditions: Boolean)
  private[core] case class Logging(console_trace_level: String, file_trace_level: String)

  def parseLoggingFromPayload(payload: Config) = {
    Logging(console_trace_level = payload.as[String]("console_trace_level"), file_trace_level = payload.as[String]("file_trace_level"))
  }

  def parseDagSettingFromPayload(payload: Config) = {
    DagSetting(attempts = payload.as[Int]("attempts"), concurrency = payload.as[Int]("concurrency"),
      heartbeat_cycle = payload.as[FiniteDuration]("heartbeat_cycle"), cooldown = payload.as[FiniteDuration]("cooldown"),
      disable_assertions = payload.as[Boolean]("disable_assertions"), ignore_conditions = payload.as[Boolean]("ignore_conditions"))
  }

}



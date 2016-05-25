package tech.artemisia.core

import java.io.{File, FileNotFoundException}

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import tech.artemisia.TestSpec
import tech.artemisia.dag.Message.TaskStats
import tech.artemisia.util.FileSystemUtil
import tech.artemisia.util.FileSystemUtil.{FileEnhancer, withTempDirectory}
import tech.artemisia.util.HoconConfigUtil.Handler


/**
*  Created by chlr on 12/4/15.
*/
class AppContextTestSpec extends TestSpec {

  var cmd_line_params: AppSetting = _
  var sys_var:(String,String) = _
  var app_context: AppContext = _
  var os_util: testEnv.TestOsUtil = _


  override def beforeEach(): Unit = {
    os_util  = env.osUtil.asInstanceOf[testEnv.TestOsUtil]
    sys_var = Keywords.Config.GLOBAL_FILE_REF_VAR -> this.getClass.getResource("/global_config.conf").getFile
    cmd_line_params = AppContextTestSpec.defualtTestCmdLineParams
  }

  "The Config Object" must s"Read the Global File and merge it with default config file" in {

    os_util.withSysVar(Map(sys_var)) {
      app_context = new AppContext(cmd_line_params)
      app_context.payload = app_context.payload.resolve()
      info("checking if job_config is in effect")
      app_context.payload.as[String]("dummy_step1.config.table") must be ("dummy_table")
      info("checking if global_config is in effect")
      app_context.payload.as[String]("dummy_step1.config.dsn") must be ("mysql_database")
      info("checking if code config variable resolution")
      app_context.payload.as[Int]("dummy_step1.config.misc_param") must be (100)
      info("checking if reference config is available")
      app_context.payload.as[String]("foo") must be("bar")
    }
  }

  it must "throw an FileNotFoundException when the GLOBAL File doesn't exists" in  {

     sys_var = Keywords.Config.GLOBAL_FILE_REF_VAR  ->
      (this.getClass.getResource("/global_config.conf").getFile+"_not_exists") // refering to non-existant file

    os_util.withSysVar(Map(sys_var)) {
      info("intercepting exception")
      val ex = intercept[FileNotFoundException] {
        app_context = new AppContext(cmd_line_params)
      }
      info("validating exception message")
      ex.getMessage must be(s"The Config file ${sys_var._2} is missing")
    }
  }

  it must "throw an FileNotFoundException when the config file doesn't exist" in  {

    os_util.withSysVar(Map(sys_var)) {
      val config_file = "/not_exists_file1"
      cmd_line_params = cmd_line_params.copy(config = Some(config_file))
      info("intercepting exception")
      val ex = intercept[FileNotFoundException] {
        app_context = new AppContext(cmd_line_params)
      }
      info("validating exception message")
      ex.getMessage must be(s"The Config file $config_file is missing")
    }

  }

  it must "throw a ConfigException.Parse exception on invalid context string" in {
    cmd_line_params = cmd_line_params.copy(context = Some("a==b==c"))
    info("intercepting exception")
    intercept[ConfigException.Parse] {
      app_context = new AppContext(cmd_line_params)
    }
  }

  it must "write the checkpoint in the right file with right content" in {
    withTempDirectory("AppContextSpec") {
      workingDir => {
        val task_name = "dummy_task"
        val cmd = cmd_line_params.copy(working_dir = Some(workingDir.toString))
        app_context = new AppContext(cmd)
        app_context.commitCheckpoint(task_name, AppContextTestSpec.getTaskStatsConfigObject)
        val checkpoint = ConfigFactory.parseFile(new File(FileSystemUtil.joinPath(workingDir.toString, "checkpoint.conf")))
        info("validating end-time")
        checkpoint.getString(s"${Keywords.Checkpoint.TASK_STATES}.$task_name.${Keywords.TaskStats.END_TIME}") must be("2016-01-18 22:27:52")
        info("validating start-time")
        checkpoint.getString(s"${Keywords.Checkpoint.TASK_STATES}.$task_name.${Keywords.TaskStats.START_TIME}") must be("2016-01-18 22:27:51")
      }
    }
  }

  it must "read a checkpoint file " in {

    withTempDirectory("AppContextSpec") {
      workingDir => {
        val task_name = "dummy_task"
        val checkpointFile = new File(workingDir, "checkpoint.conf")
        checkpointFile <<=
          s"""
            |{
            |  "${Keywords.Checkpoint.PAYLOAD}": {
            |    "foo": "bar"
            |  },
            |  "${Keywords.Checkpoint.TASK_STATES}": {
            |    $task_name = {
            |      ${Keywords.TaskStats.ATTEMPT} = 1,
            |      ${Keywords.TaskStats.END_TIME} = "2016-05-23 23:11:07",
            |      ${Keywords.TaskStats.START_TIME} = "2016-05-23 23:10:56",
            |      ${Keywords.TaskStats.STATUS} = SUCCEEDED,
            |      ${Keywords.TaskStats.TASK_OUTPUT}: {
            |        "foo": "bar"
            |      }
            |    }
            |  }
            |}
          """.stripMargin
        val cmd = cmd_line_params.copy(working_dir = Some(workingDir.toString))
        app_context = new AppContext(cmd)
        val task_stats = app_context.checkpoints.taskStatRepo(task_name)
        info("validating end_time")
        task_stats.endTime must be("2016-05-23 23:11:07")
        info("validating start_time")
        task_stats.startTime must be("2016-05-23 23:10:56")
      }
    }
  }

  it must "make working_dir is configurable from cmdline" in {
    val workingDir = "/var/tmp"
    val cmdLineParam = AppContextTestSpec.defualtTestCmdLineParams.copy(working_dir = Some(workingDir))
    val appContext = new AppContext(cmdLineParam)
    appContext.workingDir must be (workingDir)
  }

  it must "make working_dir is configurable via settings node in payload" in {
    val workingDir = "/var/tmp/artemisia"
    val runID = "qwertyuiop"
    FileSystemUtil.withTempFile(fileName = "appcontext_working_dir_test") {
      file => {
        file <<=
         s"""
            |__setting__.core.working_dir = $workingDir
            |
            |step1 = {
            | Component = SomeDummyComponent
            |}
          """.stripMargin
        val appSetting = AppSetting(cmd=Some("run"), value = Some(file.toString), run_id = Some(runID))
        info(appSetting.value.get)
        val appContext = new AppContext(appSetting)
        appContext.workingDir must be (FileSystemUtil.joinPath(workingDir,runID))
      }
    }
  }


}



object AppContextTestSpec {

  def getTaskStatsConfigObject = {
    val task_stat_config: Config = ConfigFactory parseString s"""
        |{
        |    ${Keywords.TaskStats.ATTEMPT} = 1,
        |    ${Keywords.TaskStats.END_TIME} = "2016-01-18 22:27:52",
        |    ${Keywords.TaskStats.START_TIME} = "2016-01-18 22:27:51",
        |    ${Keywords.TaskStats.STATUS} = "SUCCEEDED",
        |    ${Keywords.TaskStats.TASK_OUTPUT} = {"new_variable": 1000 }
        |}
      """.stripMargin

    TaskStats(task_stat_config)
  }


  def defualtTestCmdLineParams = {

    val job_config = Some(this.getClass.getResource("/job_config.conf").getFile)
    val code = Some(this.getClass.getResource("/code/code_with_simple_mysql_component.conf").getFile)
    val context = Some("ignore_failure=yes")
    val working_dir = None
    val cmd_line_params = AppSetting(cmd=Some("run"), value=code, context = context, config = job_config,
      working_dir = working_dir)
    cmd_line_params

  }


}

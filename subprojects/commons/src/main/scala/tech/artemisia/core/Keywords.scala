package tech.artemisia.core

import tech.artemisia.util.FileSystemUtil

/**
 * Created by chlr on 1/1/16.
 */
object Keywords {

  val APP = "Artemisia"

  object ActorSys  {
    val CUSTOM_DISPATCHER = "balancing-pool-router-dispatcher"
  }

  object Config {
    val GLOBAL_FILE_REF_VAR = "ARTEMISIA_CONFIG"
    val SETTINGS_SECTION = "__settings__"
    val CONNECTION_SECTION = "__connections__"
    val USER_DEFAULT_CONFIG_FILE = FileSystemUtil.joinPath(System.getProperty("user.home"), "artemisia.conf")
    val CHECKPOINT_FILE = "checkpoint.conf"
    val DEFAULTS = "__defaults__"
    val WORKLET = "__worklet__"
    val SYSTEM_DEFAULT_CONFIG_FILE_JVM_PARAM = "setting.file"
  }

  object DagEditor {
    val Component = "DagEditor"
    val Task = "Import"
  }

  object Connection {
    val HOSTNAME = "host"
    val USERNAME = "username"
    val PASSWORD = "password"
    val DATABASE = "database"
    val PORT = "port"
  }

  object Task {
    val COMPONENT = "Component"
    val TASK = "Task"
    val DEPENDENCY = "dependencies"
    val IGNORE_ERROR = "ignore-error"
    val COOLDOWN = "cooldown"
    val ATTEMPT = "attempts"
    val PARAMS = "params"
    val CONDITION = "when"
    val VARIABLES = "define"
    val ASSERTION = "assert"
    val ITERATE = "forall"
  }


  object TaskStats {
    val STATS = "__stats__"
    val QUEUE_TIME = "queue-time"
    val START_TIME = "start-time"
    val END_TIME = "end-time"
    val STATUS = "status"
    val DURATION = "duration"
    val ATTEMPT = "attempts"
    val TASK_OUTPUT = "task_output"
  }

  object Checkpoint {
    val TASK_STATES = "__taskstates__"
    val PAYLOAD = "__payload__"
  }
}

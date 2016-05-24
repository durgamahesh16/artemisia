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
    val DEFAULT_GLOBAL_CONFIG_FILE = FileSystemUtil.joinPath(System.getProperty("user.home"), "artemisia.conf")
    val CHECKPOINT_FILE = "checkpoint.conf"
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
    val SKIP_EXECUTION = "skip-execution"
    val COOLDOWN = "cooldown"
    val ATTEMPT = "attempts"
    val PARAMETERS = "params"
    val PARAMS = "params"
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

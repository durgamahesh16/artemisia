package tech.artemesia.core

import tech.artemesia.util.FileSystemUtil

/**
 * Created by chlr on 1/1/16.
 */
object Keywords {

  val APP = "Artemesia"

  object ActorSys  {
    val CUSTOM_DISPATCHER = "balancing-pool-router-dispatcher"
  }

  object Config {
    val GLOBAL_FILE_REF_VAR = "ARTEMESIA_CONFIG"
    val SETTINGS_SECTION = "__settings__"
    val CONNECTION_SECTION = "__connections__"
    val DEFAULT_GLOBAL_CONFIG_FILE = FileSystemUtil.joinPath(System.getProperty("user.home"), "artemesia.conf")
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
    val IGNORE_ERROR = "ignore_error"
    val SKIP_EXECUTION = "skip_execution"
    val COOLDOWN = "cooldown"
    val ATTEMPT = "attempts"
    val PARAMETERS = "params"
    val PARAMS = "params"
  }

  object TaskStats {
    val STATS = "__stats__"
    val QUEUE_TIME = "queue_time"
    val START_TIME = "start_time"
    val END_TIME = "end_time"
    val STATUS = "status"
    val DURATION = "duration"
    val ATTEMPT = "attempts"
    val TASK_OUTPUT = "task_output"
  }
}

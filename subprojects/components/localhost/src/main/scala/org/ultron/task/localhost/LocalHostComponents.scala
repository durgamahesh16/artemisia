package org.ultron.task.localhost

import com.typesafe.config.Config
import org.ultron.task.{Task, Component}


/**
 * Created by chlr on 2/21/16.
 */
class LocalHostComponents extends Component {
  override def dispatch(task: String,config: Config): Task = {
    task match {
      case "ScriptTask" => ScriptTask(config)
    }
  }
}

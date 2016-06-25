package tech.artemisia.task.localhost

import com.typesafe.config.ConfigFactory
import tech.artemisia.core.Keywords
import tech.artemisia.task.Component


/**
 * Created by chlr on 2/21/16.
 */
class CoreComponent(componentName: String) extends Component(componentName) {

  override val info = s"Component that supports core tasks of ${Keywords.APP}"

  override val defaultConfig = ConfigFactory.empty()

  override val tasks = Seq(ScriptTask, EmailTask, SFTPTask)

}

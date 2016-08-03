package tech.artemisia.task.hadoop.hive

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.{Component, TaskLike}

/**
  * Created by chlr on 8/2/16.
  */
class HiveComponent(componentName: String) extends Component(componentName) {

  override val tasks: Seq[TaskLike] = HQLExecute :: HQLExport :: HQLRead :: Nil

  override val defaultConfig: Config = ConfigFactory.empty()

  override val info: String = "A component for hive interaction"

}

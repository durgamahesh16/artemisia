package tech.artemisia.core

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.Component
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 7/14/16.
  */
class ReferenceGenerator {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory parseFile new File(args(0))
    val components = config.getConfig(s"${Keywords.Config.SETTINGS_SECTION}.components").asMap[String](s"${Keywords.Config.SETTINGS_SECTION}.components") map {
      case (name,component) => { (name, Class.forName(component).getConstructor(classOf[String]).newInstance(name).asInstanceOf[Component] ) }
    }
    components.foldLeft(ConfigFactory.empty()){ (x: Config, y: Config) => x withFallback y }
  }

}

package tech.artemisia.core

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.Component
import tech.artemisia.util.HoconConfigUtil
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 7/14/16.
  */
object ReferenceGenerator {

  def main(args: Array[String]): Unit = {
    val globalConfig = ConfigFactory parseFile new File(args(0))
    val components = globalConfig.asMap[String](s"${Keywords.Config.SETTINGS_SECTION}.components") map {
      case (name,component) => { Class.forName(component).getConstructor(classOf[String]).newInstance(name).asInstanceOf[Component] }
    }
   val result = components.foldLeft(ConfigFactory.empty()){ (carry: Config, y: Component) => y.consolidateDefaultConfig withFallback carry }
    val writer = new BufferedWriter(new FileWriter(args(0)))
    writer.write(HoconConfigUtil.render(globalConfig.withValue(Keywords.Config.DEFAULTS, result.root()).root()))
    writer.close()
  }

}

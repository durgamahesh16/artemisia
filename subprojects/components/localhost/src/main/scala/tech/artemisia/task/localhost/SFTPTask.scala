package tech.artemisia.task.localhost

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.task.localhost.util.SFTPHelper
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.FileSystemUtil.joinPath

import scala.collection.JavaConverters._

/**
  * Created by chlr on 6/22/16.
  */
class SFTPTask(name: String, connection: SFTPConnection, remoteToLocal: Seq[(String, String)], localToRemote: Seq[(String, String)] )
  extends Task(name) {

  override protected[task] def setup(): Unit = {}

  override protected[task] def work(): Config = {
   val helper = new SFTPHelper(connection)
    remoteToLocal foreach {
      case (x,y) => helper.copyToLocal(x,y)
    }
    localToRemote foreach {
      case (x,y) => helper.copyFromLocal(x,y)
    }
    ConfigFactory.empty()
  }

  override protected[task] def teardown(): Unit = {}

}

object SFTPTask extends TaskLike {

  override val taskName: String = "SFTPTask"

  override def apply(name: String, config: Config): Task = {
    val remoteToLocal =  config.as[List[ConfigValue]]("put") map { _.unwrapped() } map {
      case (key, value: String) => key -> value
      case remote: String =>  remote -> joinPath(System.getProperty("user.dir"),Paths.get(remote).getFileName.toString)
    }
    val localToRemote = config.as[List[ConfigValue]]("get") map { _.unwrapped() } map {
      case (key, value: String) => key -> value
      case remote: String =>  remote -> joinPath(System.getProperty("user.dir"),Paths.get(remote).getFileName.toString)
    }
    val connection = SFTPConnection(config.as[Config])
    SFTPTask(name,)
  }

  override def configStructure(component: String): String = ???

  override val fieldDefinition: Seq[(String, AnyRef)] = _

  override val info: String = _

  override val desc: String = _

}


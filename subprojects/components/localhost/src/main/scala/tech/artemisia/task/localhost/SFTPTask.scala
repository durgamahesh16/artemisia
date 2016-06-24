package tech.artemisia.task.localhost

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import tech.artemisia.core.Keywords
import tech.artemisia.task.localhost.util.SFTPHelper
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.HoconConfigUtil.Handler
import scala.collection.JavaConverters._
import tech.artemisia.util.DocStringProcessor.StringUtil

/**
  * Created by chlr on 6/22/16.
  */
class SFTPTask(name: String, connection: SFTPConnection, remoteToLocal: Seq[(String, Option[String])], localToRemote: Seq[(String, Option[String])],
              lcd: Option[String] = None)
  extends Task(name) {

  override protected[task] def setup(): Unit = {}

  override protected[task] def work(): Config = {
   val helper = new SFTPHelper(connection)
    lcd foreach { helper.setLCD }
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

  val remoteToLocal: Seq[(String, Option[String])] =  config.as[List[AnyRef]]("put") map {
    case x: java.util.Map[String, String] @unchecked => x.asScala.toSeq.map(a => a._1 -> Some(a._2)).head
    case x: String =>  x -> None
  }
  val localToRemote = config.as[List[AnyRef]]("get") map {
    case x: java.util.Map[String, String] @unchecked => x.asScala.toSeq.map(a => a._1 -> Some(a._2)).head
    case x: String =>  x -> None
  }
  val connection = SFTPConnection.parseConnectionProfile(config.as[ConfigValue]("connection"))
  new SFTPTask(name,connection, remoteToLocal, localToRemote)
}

  override def configStructure(component: String): String =
    s"""
       | {
       |   ${Keywords.Task.COMPONENT} = $component
       |   ${Keywords.Task.COMPONENT} = $taskName
       |   params = {
       |      connection = ${SFTPConnection.configStructure.ident(15)}
       |      get = [{ 'root_sftp_dir/file1.txt' = '/var/tmp/file1.txt' },
       |              'root_sftp_dir/file2.txt' ]
       |        @type(array)
       |      put = [
       |          { '/var/tmp/file1.txt' = 'sftp_root_dir/file1.txt' },
       |          '/var/tmp/file1.txt'
       |       ] @type(array)
       |   }
       | }
     """.stripMargin


  override val fieldDefinition: Seq[(String, AnyRef)] = Seq(  
    "connection" -> SFTPConnection.fieldDefinition,
    "get" -> "array of object or strings providing source and target (optional if type is string) paths",
    "put" -> "array of object or strings providing source and target (optional if type is string) paths"
  )


  override val info: String = s"$taskName supports copying files from remote sftp server to local filesystem and vice versa"

  override val desc: String =
    s"""
       |
     """.stripMargin

}


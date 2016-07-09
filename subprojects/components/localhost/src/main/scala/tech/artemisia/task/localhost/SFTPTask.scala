package tech.artemisia.task.localhost

import java.nio.file.{Path, Paths}

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import tech.artemisia.task.localhost.util.SFTPManager
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.DocStringProcessor.StringUtil
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.collection.JavaConverters._

/**
  * Created by chlr on 6/22/16.
  */
class SFTPTask(name: String, val connection: SFTPConnection, val remoteToLocal: Seq[(Path, Option[Path])], val localToRemote: Seq[(Path, Option[Path])],
               val localWorkingDir: Option[String] = None, val remoteWorkingDir: Option[String] = None)
  extends Task(name) {

  val manager = new SFTPManager(connection)

  override protected[task] def setup(): Unit = {}

  override protected[task] def work(): Config = {

    localWorkingDir foreach { manager.setLCD }
    remoteWorkingDir foreach { manager.setRCD }
    remoteToLocal foreach {
      case (x,y) => manager.copyToLocal(x,y)
    }
    localToRemote foreach { case (x,y) => manager.copyFromLocal(x,y) }
    ConfigFactory.empty()
  }

  override protected[task] def teardown(): Unit = {
    manager.terminate()
  }

}

object SFTPTask extends TaskLike {

  override val taskName: String = "SFTPTask"

  override def apply(name: String, config: Config): Task = {

    def parseFileMapping(mode: String) = {

       config.hasPath(mode) match {
          case true => config.as[List[AnyRef]](mode) map {
            case x: java.util.Map[String, String] @unchecked => x.asScala.toSeq.map(a => Paths.get(a._1) -> Some(Paths.get(a._2))).head
            case x: String =>  Paths.get(x) -> None
          }
          case false => Nil
       }
    }

    new SFTPTask(name,
        SFTPConnection.parseConnectionProfile(config.as[ConfigValue]("connection")),
        parseFileMapping("get"),
        parseFileMapping("put"),
        config.getAs[String]("local-dir"),
        config.getAs[String]("remote-dir")
    )

  }



  override val paramConfigDoc =  ConfigFactory parseString
    s"""| {
        |   params = {
        |      ${SFTPConnection.configStructure.ident(15)}
        |      get = "[{ 'root_sftp_dir/file1.txt' = '/var/tmp/file1.txt' },'root_sftp_dir/file2.txt'] @type(array)"
        |      put = "[{ '/var/tmp/file1.txt' = 'sftp_root_dir/file1.txt' },'/var/tmp/file1.txt'] @type(array)"
        |      local-dir = "/var/tmp @default(your current working directory.) @info(current working directory)"
        |      remote-dir = "/root @info(remote working directory)"
        |   }
        | }
     """.stripMargin


  override val fieldDefinition = Map(
    "connection" -> SFTPConnection.fieldDefinition,
    "get" -> "array of object or strings providing source and target (optional if type is string) paths",
    "put" -> "array of object or strings providing source and target (optional if type is string) paths",
    "local-dir" -> "set local working directory. by default it will be your current working directory",
    "remote-dir" -> "set remote working directory"
  )


  override val info: String = s"$taskName supports copying files from remote sftp server to local filesystem and vice versa"

  override val desc: String =
    s"""
       |
     """.stripMargin

}


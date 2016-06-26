package tech.artemisia.task.localhost

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import tech.artemisia.TestSpec
import tech.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 6/25/16.
  */
class SFTPTaskSpec extends TestSpec with BeforeAndAfterAll {

  val connection = SFTPConnection("127.0.0.1", MockSFTPServer.port, "artemisia", Some("password"))

  override def beforeAll() = {
    MockSFTPServer.start()
  }

  "SFTPTask" must "upload local file to remote" in {
      withTempFile(fileName = "stptask1") {
         file => {
           val task = new SFTPTask("sftp_task", connection, Nil ,
             file.toPath -> None :: file.toPath -> Some(Paths.get("payload1.txt")) :: file.toPath -> Some(Paths.get("payload2.txt"))  :: Nil,
           remoteWorkingDir = Some("test"))
           task.execute()
           new File(joinPath(MockSFTPServer.rootDir.toPath.toString, "test", file.toPath.getFileName.toString)).exists() must be (true)
           new File(joinPath(MockSFTPServer.rootDir.toPath.toString, "test", "payload1.txt")).exists() must be (true)
           new File(joinPath(MockSFTPServer.rootDir.toPath.toString, "test", "payload2.txt")).exists() must be (true)
        }
      }
    }

  it must "download files from the remote to local" in {
    withTempDirectory(directoryName = "stptask2") {
      file => {
        val task = new SFTPTask("sftp_task", connection,
          Paths.get("payload1.txt") -> None :: Paths.get("payload2.txt") -> Some(Paths.get("payload3.txt")) :: Nil,
          Nil,
          remoteWorkingDir = Some("test"),
          localWorkingDir = Some(file.toPath.toAbsolutePath.toString))
          task.execute()
          info(joinPath(file.toString, "payload1.txt"))
        new File(joinPath(file.toString, "payload1.txt")).exists() must be (true)
        new File(joinPath(file.toString, "payload3.txt")).exists() must be (true)
      }
    }
  }

  it must "parse config object " in {
         val config = ConfigFactory parseString
           s"""
            | {
            |   connection = {
            |      hostname = "sftp-server"
            |      port = 2222
            |      username = artemisia
            |      password = caria
            |   }
            |   local-dir = "/var/tmp"
            |   remote-dir = "sftp/root"
            |   put = [
            |        { "file1.txt" = "file2.txt" }
            |        "file3.txt"
            |     ]
            |   get = [
            |      { "file1.txt" = "file2.txt" }
            |       "file3.txt"
            |   ]
            | }
         """.
           stripMargin
         val task =  SFTPTask("sftptask", config).asInstanceOf[SFTPTask]
         task.connection.host must be("sftp-server")
         task.connection.port must be(2222)
         task.connection.password must be (Some("caria"))
         task.connection.username must be ("artemisia")
         task.localWorkingDir must be (Some("/var/tmp"))
         task.remoteWorkingDir must be (Some("sftp/root"))
         task.localToRemote.head must be (Paths.get("file1.txt") -> Some(Paths.get("file2.txt")))
         task.localToRemote(1) must be (Paths.get("file3.txt") -> None)
         task.remoteToLocal.head must be (Paths.get("file1.txt") -> Some(Paths.get("file2.txt")))
         task.remoteToLocal(1) must be (Paths.get("file3.txt") -> None)
  }

  override def afterAll() = {
    MockSFTPServer.close()
    info("SFTP Server shutdown")
  }

}

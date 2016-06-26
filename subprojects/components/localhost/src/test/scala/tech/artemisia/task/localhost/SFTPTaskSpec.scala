package tech.artemisia.task.localhost

import java.io.File
import java.nio.file.Paths

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

  it must "parse config oject "

  override def afterAll() = {
    MockSFTPServer.close()
    info("SFTP Server shutdown")
  }

}

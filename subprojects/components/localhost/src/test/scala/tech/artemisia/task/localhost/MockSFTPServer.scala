package tech.artemisia.task.localhost

import java.io.File

import com.google.common.io.Files
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory
import tech.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 6/25/16.
  */
object MockSFTPServer {

  val rootDir: File = Files.createTempDir()
  val port: Int = 30564

  val sshd = SshServer.setUpDefaultServer()
  sshd.setPort(port)
  sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider())
  sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
    def authenticate(username: String, password: String, session: ServerSession) = {
      password == "password"
    }
  })
  sshd.setSubsystemFactories(java.util.Collections.singletonList(new SftpSubsystemFactory))
  sshd.setFileSystemFactory(new VirtualFileSystemFactory(rootDir.toPath))

  def start() =  {
    new File(joinPath(rootDir.toString, "test")).mkdir()
    sshd.start()
  }

  def close() = {
    rootDir.delete()
    sshd.close()
  }

}

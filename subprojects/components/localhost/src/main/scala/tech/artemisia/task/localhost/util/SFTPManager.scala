package tech.artemisia.task.localhost.util

import java.nio.file.{Path, Paths}

import com.jcraft.jsch.{ChannelSftp, JSch}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.localhost.SFTPConnection
import tech.artemisia.util.FileSystemUtil.joinPath

/**
  * Created by chlr on 6/22/16.
  */
class SFTPManager(connection: SFTPConnection) {

  val jsch = new JSch()
  connection.pkey foreach { x => jsch.addIdentity(x.getAbsolutePath) }

  private lazy val session = {
    val session = jsch.getSession(connection.username, connection.host, connection.port)
    val props = new java.util.Properties()
    props.put("StrictHostKeyChecking", "no")
    session.setConfig(props)
    session
  }

  private lazy val sftpChannel = {
    connection.password foreach { x => session.setPassword(x) }
    session.connect()
    val channel = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    channel.connect()
    channel
  }

  def setLCD(path: String) = {
    sftpChannel.lcd(path)
  }

  def setRCD(path: String) = {
    sftpChannel.cd(path)
  }

  def copyToLocal(remote: Path, local: Option[Path] = None) = {
    AppLogger info s"copying remote file $remote to ${local.getOrElse(Paths.get(remote.toString).toAbsolutePath).toString}"
    local match {
      case Some(x) => sftpChannel.get(remote.toString, x.toString)
      case None => sftpChannel.get(remote.toString)
    }
  }

  def copyFromLocal(local: Path, remote: Option[Path] = None) = {
    AppLogger info s"copying local file $local to remote at ${remote.getOrElse(joinPath(sftpChannel.pwd(), local.toAbsolutePath.getFileName.toString))} "

    remote match {
      case Some(x) => sftpChannel.put(local.toString, x.toString)
      case None => sftpChannel.put(local.toString, joinPath(sftpChannel.pwd(), local.toAbsolutePath.getFileName.toString))
    }
  }

  def terminate() = {
    AppLogger debug "closing sftp channel"
    if (sftpChannel.isConnected) sftpChannel.exit()
    AppLogger debug "closing ssh channel"
    if (session.isConnected) session.disconnect()
  }

}

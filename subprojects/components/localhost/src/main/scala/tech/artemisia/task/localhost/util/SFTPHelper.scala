package tech.artemisia.task.localhost.util

import com.jcraft.jsch.{ChannelSftp, JSch}
import tech.artemisia.task.localhost.SFTPConnection

/**
  * Created by chlr on 6/22/16.
  */
class SFTPHelper(connection: SFTPConnection) {

  val jsch = new JSch()
  connection.pkey foreach { x => jsch.addIdentity(x.getAbsolutePath) }


  private lazy val sftpChannel = {
    val session = jsch.getSession(connection.username, connection.host, connection.port)
    connection.password foreach { x => session.setPassword(x) }
    session.connect()
    val channel = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    channel.connect()
    channel
  }

  def setLCD(path: String) = {
    sftpChannel.lcd(path)
  }

  def copyToLocal(remote: String, local: Option[String]) = {
    local match {
      case Some(x) => sftpChannel.get(remote, x)
      case None => sftpChannel.get(remote)
    }
  }

  def copyFromLocal(local: String, remote: Option[String]) = {
    remote match {
      case Some(x) => sftpChannel.put(local, x)
      case None => sftpChannel.put(local)
    }
  }

}

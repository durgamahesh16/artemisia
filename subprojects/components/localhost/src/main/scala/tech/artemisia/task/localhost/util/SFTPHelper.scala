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
    val channel = session.openChannel("sftp")
    channel.connect()
    channel.asInstanceOf[ChannelSftp]
  }

  def copyToLocal(remote: String, local: String) = {
    sftpChannel.get(remote, local)
  }

  def copyFromLocal(local: String, remote: String) = {
    sftpChannel.put(local, remote)
  }

}

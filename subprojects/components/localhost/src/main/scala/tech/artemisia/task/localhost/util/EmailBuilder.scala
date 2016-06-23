package tech.artemisia.task.localhost.util

import org.apache.commons.mail.{Email, EmailAttachment, MultiPartEmail}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.localhost.{EmailConnection, EmailRequest}

/**
 * Created by chlr on 6/16/16.
 */

class EmailBuilder(val emailConnection: Option[EmailConnection] = None) {

  val email = new MultiPartEmail()

  def build(emailRequest: EmailRequest): Email = {
    configureConnection(emailRequest)
    configureRequest(emailRequest)
    email
  }

  private[localhost] def configureConnection(emailRequest: EmailRequest) = {
    emailConnection match {
      case Some(connection) => {
        email.setHostName(connection.host)
        email.setSmtpPort(connection.port)
        email.setSSL(connection.ssl)
        email.setTLS(connection.tls)
        for (username <- connection.username; password <- connection.password) {
          email.setAuthentication(username, password)
        }
        connection.from foreach { x => email.setFrom(x) }
        connection.replyTo foreach { x => email.addReplyTo(x) }
      }
      case None => {
        AppLogger warn s"Email connection not set. raising Exception"
        throw new RuntimeException("Email connection not set")
      }
    }
  }

  private[localhost] def configureRequest(emailRequest: EmailRequest) = {

    emailRequest.to foreach { email.addTo }
    emailRequest.cc foreach { email.addCc }
    emailRequest.bcc foreach { email.addBcc }

    emailRequest.attachments foreach {
      case (x,y) =>
        val attachment = new EmailAttachment()
        attachment.setPath(y.toPath.toString)
        x foreach { a => attachment.setName(a) }
        email.attach(attachment)
    }

    email.setSubject(emailRequest.subject)
    email.setMsg(emailRequest.message)

  }


}



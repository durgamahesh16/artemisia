package tech.artemisia.task.localhost

import javax.naming.Context
import javax.naming.directory.InitialDirContext
import org.apache.commons.mail.{Email, EmailAttachment, MultiPartEmail}

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
      case None => ()
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


  private def getMXServer(domainName: String): Seq[String] = {
    val env = new java.util.Hashtable[String, Object]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory")
    env.put(Context.PROVIDER_URL, "dns:")
    val ctx = new InitialDirContext(env)
    val attribute = ctx.getAttributes(domainName, Array[String] {"MX"})
    attribute.get("MX") match {
      case null => Seq(domainName)
      case x => {
        val buffer = for (idx <- 1 to x.size()) yield { ("" + x.get(idx-1)).split("\\s+") }
        buffer sortWith { (a,b) =>  Integer.parseInt(a(0)) > Integer.parseInt(b(0)) } map
          { x => if(x(1).endsWith(".")) x(1).substring(0, x(1).length - 1) else x(1) }
      }
    }
  }



}

package tech.artemisia.task.localhost

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.mail.MultiPartEmail
import tech.artemisia.core.AppLogger
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.DocStringProcessor._

/**
 * Created by chlr on 6/15/16.
 */
class EmailTask(name: String, val emailRequest: EmailRequest, val emailConnection: Option[EmailConnection]) extends Task(name) {

  val email = new MultiPartEmail()

  override protected[task] def setup(): Unit = {}

  override protected[task] def work(): Config = {
    val builder = new EmailBuilder(emailConnection)
    val email = builder.build(emailRequest)
    AppLogger info s"""sending email to ${emailRequest.to mkString ","}"""

    email.send()
    ConfigFactory.empty()
  }

  override protected[task] def teardown(): Unit = {}

}


object EmailTask extends TaskLike {

  override val taskName: String = "EmailTask"

  override val info: String = s"$taskName is used to send Emails."

  override val desc: String = ""

  override def configStructure(component: String): String = {
    s"""
       |Component = $component
       |Task = $taskName
       |params = {
       |	  connection = <% email_connection
       |                <------------------->
       |                ${EmailConnection.structure.ident(18)}
       |                 %> @type(str, obj)
       |	  email = ${EmailRequest.structure.ident(18)}
       |}
     """.stripMargin
  }

  override val fieldDefinition = Seq(
    "connection" -> "TODO",
    "email" -> "TODO"
  )

  override def apply(name: String, config: Config): Task = {
    val emailConnection = if (config.hasPath("connection")) Some(EmailConnection.parseConnectionProfile(config.getValue("connection"))) else None
    val emailRequest = EmailRequest(config.as[Config]("email"))
    new EmailTask(name, emailRequest, emailConnection)
  }
}
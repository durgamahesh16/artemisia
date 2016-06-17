package tech.artemisia.task.localhost

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.mail.MultiPartEmail
import tech.artemisia.task.{Task, TaskLike}

/**
 * Created by chlr on 6/15/16.
 */
class EmailTask(name: String, val emailRequest: EmailRequest, val emailConnection: Option[EmailConnection]) extends Task(name) {

  val email = new MultiPartEmail()

  override protected[task] def setup(): Unit = {}

  override protected[task] def work(): Config = {
    val builder = new EmailBuilder(emailConnection)
    val email = builder.build(emailRequest)
    email.send()
    ConfigFactory.empty()
  }

  override protected[task] def teardown(): Unit = {}

}


object EmailTask extends TaskLike {

  override val taskName: String = "EmailTask"

  override val info: String = "This Task sends Emails"

  override def doc(component: String): String = ???

  override def apply(name: String, config: Config): Task = {

  }

}

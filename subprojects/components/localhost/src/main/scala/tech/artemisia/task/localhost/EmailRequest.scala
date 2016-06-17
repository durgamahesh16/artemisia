package tech.artemisia.task.localhost

import java.io.File

import com.typesafe.config.Config
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.collection.JavaConverters._

/**
 * Created by chlr on 6/15/16.
 */

/**
 *
 * @param to to address list
 * @param cc cc address list
 * @param bcc bcc address list
 * @param attachments list of attachment files
 * @param subject subject
 * @param message message
 */
case class EmailRequest(to: Seq[String], cc: Seq[String] = Nil, bcc: Seq[String] =  Nil,
                        attachments: Seq[(Option[String], File)] = Nil, subject: String, message: String)


object EmailRequest {

  def apply(config: Config) = {

    def parseAddressConfig(field: String) = {
      config.getValue("to").unwrapped() match {
        case x: String => Seq(x)
        case x: java.util.List[String] @unchecked => x.asScala
      }
    }

    EmailRequest(
      to = parseAddressConfig("to"),
      cc = parseAddressConfig("cc"),
      bcc = parseAddressConfig("bcc"),
      attachments = config.as[Seq[AnyRef]]("attachments") map {
        case x: String => None ->new File(x)
        case y: java.util.Map[String,String] =>  y.asScala.toSeq.head match { case (a: String, b: String) => Some(a) -> new File(b) }
      },
      subject = config.as[String]("subject"),
      message = config.as[String]("message")
    )
  }



}

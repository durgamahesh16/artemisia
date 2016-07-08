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
case class  EmailRequest(to: Seq[String], cc: Seq[String] = Nil, bcc: Seq[String] =  Nil,
                        attachments: Seq[(Option[String], File)] = Nil, subject: String, message: String)


object EmailRequest {

  val structure =
    s"""|{
        | "to_[0]" = "xyz@example.com"
        | "to_[1]" = [ "xyz1@example.com", "xyz2@example.com" ]
        | "cc_[0]" = "xyz@example.com @optional"
        | "cc_[1]" = "[ xyz1@example.com, xyz2@example.com ] @optional"
        | "bcc_[0]" = "xyz@example.com @optional"
        | "bcc_[1]" = "[ xyz1@example.com, xyz2@example.com ] @optional"
        | "attachment_[0]" = "['/var/tmp/file1.txt', '/var/tmp/file2.txt'] @optional"
        | "attachment_[1]" = "[{'attachment1.txt', '/var/tmp/file1.txt'}, {'attachment2.txt', '/var/tmp/file2.txt'}] @optional"
        | subject = "subject"
        | message = "message"
        |}""".stripMargin


  val fieldDefinition = Seq (
    "to" -> "to address list. it can either be a single email address string or an array of email address",
    "cc" -> "cc address list. same as to address both string and array is supported",
    "bcc" -> "bcc address list. same as to address both string and array is supported",
    "attachment" -> "can be a array of strings which w"
  )


  def apply(config: Config): EmailRequest = {

    def parseAddressConfig(field: String) = {
      config.getValue(field).unwrapped() match {
        case x: String => Seq(x)
        case x: java.util.List[String] @unchecked => x.asScala
      }
    }

    EmailRequest(
      to = parseAddressConfig("to"),
      cc = parseAddressConfig("cc"),
      bcc = parseAddressConfig("bcc"),
      attachments = if (config.hasPath("attachments")) config.as[List[AnyRef]]("attachments") map {
        case x: String => None ->new File(x)
        case y: java.util.Map[String,String] @unchecked =>  y.asScala.toSeq.head match { case (a: String, b: String) => Some(a) -> new File(b) }
      } else Nil,
      subject = config.as[String]("subject"),
      message = config.as[String]("message")
    )

  }



}

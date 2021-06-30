package com.example.notificator.notificationservice

import com.example.notificator.Event
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import java.util.{ Date, Properties }
import javax.mail._
import javax.mail.internet.{ InternetAddress, MimeMessage }

object SenderOfLetters {

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  def sendMail(recipient: String, event: Event): Unit = {
    val properties = new Properties()
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")
    properties.put("mail.smtp.host", "smtp.gmail.com")
    properties.put("mail.smtp.port", "587")

    val myAccountEmail = "xxx@gmail.com"
    val password       = "xxx"

    val session = Session.getInstance(
      properties,
      new Authenticator() {
        override protected def getPasswordAuthentication: PasswordAuthentication = {
          new PasswordAuthentication(myAccountEmail, password)
        }
      }
    )

    val message = prepareMessage(session, myAccountEmail, recipient, event)
    try {
      Transport.send(message)
    } catch {
      case ex: Exception =>
        logger.error(
          s"Error: The message was not sent, error: $ex"
        )
    }
  }

  private def prepareMessage(session: Session, myAccountEmail: String, recipient: String, event: Event) = {
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(myAccountEmail))
    message.setSubject("Application status from bank N")
    message.setContent(
      s"""
         |<h1> Dear Client </h1>
         |<h2> Email for user with id: ${event.externalClientId.split("-")(1)}  </h2>
         |<h2> Today ${new SimpleDateFormat()
           .format(event.data)} your application has received the status: ${event.stage}  </h2>
         |<h2> Processing: ${if (event.status) {
           "completed"
         } else {
           "not completed"
         }}   </h2>
         |<h3> Thanks for choosing us ! </h3>
         |""".stripMargin,
      "text/html"
    )
    message.setRecipient(Message.RecipientType.TO, new InternetAddress(recipient))
    message.setSentDate(new Date())

    message
  }
}

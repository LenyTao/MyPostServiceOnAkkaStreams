package com.example.notificator.producer

import com.example.notificator.Event
import java.util.Date

object TestEventCollection {

  private val testCollection = Seq(
    Event("1234-00155", "Received", status = false, new Date()),
    Event("0012-00157", "Received", status = false, new Date()),
    Event("0125-00263", "Received", status = false, new Date()),
    Event("1234-00155", "Processed", status = false, new Date()),
    Event("0012-00157", "Processed", status = false, new Date()),
    Event("0125-00263", "Processed", status = false, new Date()),
    Event("0895-00263", "Processed", status = false, new Date()),
    Event("1234-00155", "Approved", status = true, new Date()),
    Event("0012-00157", "Approved", status = true, new Date()),
    Event("0125-00263", "Approved", status = true, new Date())
  )

  def getTestCollection: Seq[Event] = {
    testCollection
  }
}

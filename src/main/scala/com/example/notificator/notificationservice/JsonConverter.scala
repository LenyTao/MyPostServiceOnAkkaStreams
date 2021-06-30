package com.example.notificator.notificationservice

import com.example.notificator.Event
import spray.json.{ DeserializationException, JsBoolean, JsObject, JsString, JsValue, RootJsonFormat }
import java.text.SimpleDateFormat
import java.util.Locale

trait JsonConverter {

  implicit object converterEvents extends RootJsonFormat[Event] {

    override def write(obj: Event): JsObject = JsObject(
      "externalClientId" -> JsString(obj.externalClientId),
      "stage"            -> JsString(obj.stage),
      "status"           -> JsBoolean(obj.status),
      "date"             -> JsString(obj.data.toString)
    )

    override def read(json: JsValue): Event = {
      json.asJsObject.getFields("externalClientId", "stage", "status", "date") match {
        case Seq(JsString(clientID), JsString(stage), JsBoolean(status), JsString(data)) =>
          Event(clientID, stage, status, new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH).parse(data))
        case ex =>
          throw DeserializationException(
            s"ERROR: Deserialization error occurred when trying to deserialize: $ex"
          )
      }
    }
  }
}

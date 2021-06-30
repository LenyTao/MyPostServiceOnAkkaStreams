package com.example.notificator.notificationservice

import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ConfigReader {

  case class ConnectionParameters(bootstrapServers: String, groupId: String, property: String, timeout: Int)

  private case class ClientObject(params: Config) {
    val idClient: String   = params.getString("id")
    val postClient: String = params.getString("post")
  }

  private val rootConfig = ConfigFactory.load("application.conf")

  def readConnectionParameters(): ConnectionParameters = {
    val configForConnection = rootConfig.getConfig("my.clientList.сonnectionParameters")

    val bootstrapServers = configForConnection.getString("bootstrapServers")
    val groupId          = configForConnection.getString("groupId")
    val property         = configForConnection.getString("property")
    val timeout          = configForConnection.getInt("timeout")
    ConnectionParameters(bootstrapServers, groupId, property, timeout)
  }

  def readClientList(): Map[String, String] = {
    val configForClientList = rootConfig.getConfigList("my.clientList.team.clients")
    val listClientId        = configForClientList map (ClientObject(_).idClient)
    val listClientPost      = configForClientList map (ClientObject(_).postClient)
    listClientId.zip(listClientPost).toMap
  }
}

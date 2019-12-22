package com.yuepengfei.monitor.akka.demo

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

class Master extends Actor{

  override def preStart(): Unit = {
    println("Master启动")
  }
  override def receive: Receive = {
    case "connect" =>{
      println("A work is connected")
      sender() ! "success"
    }
    case "stop" =>{
      System.exit(0)
    }
  }
}

object Master extends App{
  val host = "127.0.0.1"
  val port = "2222"

  val configStr =
    s"""
       |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.hostname = "$host"
       |akka.remote.netty.tcp.port = "$port"
     """.stripMargin
  private val config: Config = ConfigFactory.parseString(configStr)
  private val masterActorSystem = ActorSystem("masterActorSystem",config)

  private val masterActor: ActorRef = masterActorSystem.actorOf(Props(new Master),"masterActor")

//  masterActor ! "connect"

}
package com.yuepengfei.monitor.akka.demo

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Worker extends Actor{
  override def preStart(): Unit = {
    println("Worker启动")
    val selectionMaster: ActorSelection = context.actorSelection("akka.tcp://masterActorSystem@127.0.0.1:2222/user/masterActor")
    selectionMaster ! "connect"
  }
  override def receive: Receive = {
    case "success" =>{
      println("注册成功")
      sender() ! "stop"
      System.exit(0)
    }


  }
}

object Worker extends App{
  val host = "127.0.0.1"
  val port = "3333"
  val configStr =
    s"""
       |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.hostname = "$host"
       |akka.remote.netty.tcp.port = "$port"
     """.stripMargin
  val config = ConfigFactory.parseString(configStr)
  private val workerActorSystom = ActorSystem("workerActorSystom",config)
  private val workerActor: ActorRef = workerActorSystom.actorOf(Props(new Worker),"workerActor")
}



package com.yuepengfei.monitor.akka.spark

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

class Worker extends Actor{
  var selectionMaster: ActorSelection = null
  val workerId = UUID.randomUUID().toString
  override def preStart(): Unit = {
    println("worker node is starting...")
    //向主节点发送注册消息
    selectionMaster = context.actorSelection("akka.tcp://masterActorSystem@127.0.0.1:2222/user/masterActor")
    selectionMaster ! RegisterMessage(workerId,8,2)
  }
  override def receive: Receive = {
    //匹配注册成功的消息
    case RegisteredMessage(msg) =>{
      println(msg)
      //给自己发送定时任务
      import context.dispatcher
      context.system.scheduler.schedule(0 millis,5000 millis,self,HeartBeat())
    }
      //将定时心跳转发给远程的master
    case HeartBeat() =>{
      selectionMaster ! RemoteHeartBeat(workerId)
    }
  }
}

object Worker extends App{
  val host = "127.0.0.1"
  val port = "2223"
  val configStr =
    s"""
       |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.hostname = "$host"
       |akka.remote.netty.tcp.port = "$port"
     """.stripMargin
  val config = ConfigFactory.parseString(configStr)
  private val workerActorSystem = ActorSystem("masterActorSystem",config)
  private val workerActor: ActorRef = workerActorSystem.actorOf(Props(new Worker),"workerActor")

}

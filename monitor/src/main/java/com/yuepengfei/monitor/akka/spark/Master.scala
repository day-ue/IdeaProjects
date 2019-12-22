package com.yuepengfei.monitor.akka.spark

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._
class Master extends Actor{
  private var mapRegisterMessage = new mutable.HashMap[String,RegisterMessage]()
  private var mapToWorkerInfo = new mutable.HashMap[String,WorkerInfo]()
  override def preStart(): Unit = {
    println("Master is starting")
    //开启心跳时间检查
    import context.dispatcher
    context.system.scheduler.schedule(0 millis,5000 millis,self,CheckOutTime())
  }
  override def receive: Receive = {
    //处理worker的注册
    //这里可以改的更加scala风格一些
    case RegisterMessage(workerId,memory,cores) =>{
      if (!mapRegisterMessage.contains(workerId)){
        mapRegisterMessage.put(workerId,RegisterMessage(workerId,memory,cores))
        println(mapRegisterMessage)
        sender() ! RegisteredMessage("注册成功")
      }
    }
      //这里完成心跳记录
    case RemoteHeartBeat(workerId) =>{
      if (mapRegisterMessage.contains(workerId)){
        val lastTime: Long = System.currentTimeMillis()
        val registerMessage: RegisterMessage = mapRegisterMessage(workerId)
        val workerInfo = WorkerInfo(workerId,registerMessage,lastTime)
        mapToWorkerInfo.put(workerId,workerInfo)
      }
      println("发来心跳的worker数："+mapToWorkerInfo.size)
    }
      //完成自动检查心跳时间
    case CheckOutTime() =>{
      val mapOutToWorkerInfo = mapToWorkerInfo.filter(System.currentTimeMillis() - _._2.lastTime >= 2000)
      for (workerInfo <- mapOutToWorkerInfo){
        mapToWorkerInfo.remove(workerInfo._1)
        mapRegisterMessage.remove(workerInfo._1)
      }
      println("现在存活worker"+mapRegisterMessage.size+"个")
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
  val config = ConfigFactory.parseString(configStr)
  private val masterActorSystem = ActorSystem("masterActorSystem",config)
  private val masterActor: ActorRef = masterActorSystem.actorOf(Props(new Master),"masterActor")


}

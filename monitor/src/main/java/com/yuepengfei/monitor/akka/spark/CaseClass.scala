package com.yuepengfei.monitor.akka.spark

case class RegisterMessage(val workerId:String,val memory:Int,val cores:Int) extends Serializable
case class RegisteredMessage(val msg:String) extends Serializable
case class HeartBeat()
case class RemoteHeartBeat(val workerId:String) extends Serializable
case class WorkerInfo(val workerId:String,val registerMessage: RegisterMessage,val lastTime:Long)
case class CheckOutTime()
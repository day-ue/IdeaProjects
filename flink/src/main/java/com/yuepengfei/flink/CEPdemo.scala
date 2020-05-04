package com.yuepengfei.flink

import com.yuepengfei.bean.{LoginEvent, LoginWarning}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object CEPdemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val loginEventStream = env.fromCollection(List(
      new LoginEvent("1", "192.168.0.1", "fail"),
      new LoginEvent("1", "192.168.0.2", "fail"),
      new LoginEvent("1", "192.168.0.3", "fail"),
      new LoginEvent("2", "192.168.10,10", "success")
    ))

    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.type_type.equals("fail"))
        .next("next")
      .where(_.type_type.equals("fail"))
      .within(Time.seconds(1))

    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    val loginFailDataStream = patternStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {

        val second = pattern.getOrElse("next", null).iterator.next()

        new LoginWarning(second.userId, second.ip, second.type_type)
      })

    loginFailDataStream.print("second fail")

    env.execute
  }

}
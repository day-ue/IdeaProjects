package com.yuepengfei.monitor.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.tools.cmd.{Property, PropertyMapper, Reference, Spec}


object FlinkKafkaDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties
    props.put("bootstrap.servers", "192.168.240.131:9092")
    props.put("zookeeper.connect", "192.168.240.131:2181")
    props.put("group.id", "fuck")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")//value 反序列化
    props.put("auto.offset.reset", "latest")
    
    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), props))

    dataStreamSource
      .flatMap(_.split(" "))
      .map(_.trim.replace("\t", "").replace(" ", "").replace(".", ""))
      .filter(word => word != "" && word != "\n")
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .setParallelism(1)
      .print()
      .setParallelism(1)

    env.execute("Flink add data source")
  }
}

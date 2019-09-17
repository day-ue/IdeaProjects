package com.yuepengfei.monitor.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala.{Over, StreamTableEnvironment}

object FlinkTableWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val props = new Properties
    props.put("bootstrap.servers", "192.168.240.131:9092")
    props.put("zookeeper.connect", "192.168.240.131:2181")
    props.put("group.id", "fuck")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")//value 反序列化
    props.put("auto.offset.reset", "latest")

    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), props))


    val df: DataStream[WordCount] = dataStreamSource
      .flatMap(_.split(" "))
      .map(_.trim.replace("\t", "").replace(" ", "").replace(".", ""))
      .filter(word => word != "" && word != "\n")
      .map(WordCount(_, 1))
      .keyBy("word")
      .sum("num")


    tEnv.registerDataStream("word_count", df)

    val result: Table = tEnv.sqlQuery(
      """
        |select word, num from word_count
        |""".stripMargin)

    tEnv.scan("word_count").window(Over
      .partitionBy("a")
      .orderBy("rowtime")
      .preceding("UNBOUNDED_RANGE")
      .following("CURRENT_RANGE")
      .as("w"))
    tEnv.toRetractStream[WordCount](result).print()

    env.execute("FlinkTableWordCount")

  }
}

case class WordCount(word: String, num: Int)

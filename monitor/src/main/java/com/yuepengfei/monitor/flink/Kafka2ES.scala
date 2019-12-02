package com.yuepengfei.monitor.flink

import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType


object Kafka2ES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.setStateBackend(new FsStateBackend("file:/data/flink/checkpoint"))

    val props = new Properties
    props.put("bootstrap.servers", "192.168.240.131:9092")
    props.put("zookeeper.connect", "192.168.240.131:2181")
    props.put("group.id", "test1")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //value 反序列化
    props.put("auto.offset.reset", "latest")

    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), props))

    val userConf: util.HashMap[String, String] = new util.HashMap[String,String]()
    userConf.put("cluster.name", "elasticsearch")
    userConf.put("bulk.flush.max.actions", "1")

    val addressList = new util.ArrayList[InetSocketAddress]()
    addressList.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))

    dataStreamSource.print("")

    env.execute("From kafka to es")
  }
}

package org.apache.spark.streaming.kafka010

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaInputDStreamTest extends App {

  private val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
  sparkConf.set("per.partition.offsetrange.step" ,"100")
  sparkConf.set("per.partition.offsetrange.threshhold" ,"100")
  sparkConf.set("enable.auto.partition" ,"true")

  private val ssc = new StreamingContext(sparkConf, Seconds(2))

  val topics = "test".split(",").toSet
  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> this.getClass.getName,
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: lang.Boolean)
  )

  private val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferBrokers, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

  ds.transform( rdd => {
    println(s"分区数量：${rdd.getNumPartitions}")
    rdd
  }).foreachRDD(rdd => {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetRanges.foreach(o =>{
      println(s"topic: ${o.topic}, partition: ${o.partition}, from: ${o.fromOffset}, to: ${o.untilOffset}")
    })

  })

  ssc.start()
  ssc.awaitTermination()


}

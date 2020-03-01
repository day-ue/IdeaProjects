package com.yuepengfei.monitor.sparkstreaming

import java.lang

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.kafka010.AssignStrategy
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._

/**
 * 实际生产中kafka消费，维护offset
 */

object PrdKafkaWordCount extends sparkTrait {

  def main(args: Array[String]): Unit = {
    /*Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkWordCountDemo").set("spark.executor.memory", "1G")

    //实际开发中我们通常使用dataframe,所以必须创建sparksession，从而引入隐式转换
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()*/
    import spark.implicits._

    val scc = new StreamingContext(spark.sparkContext, Seconds(10))
    scc.checkpoint("./data/checkpoint/PrdKafkaWordCount")

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "192.168.240.131:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    var ds: InputDStream[ConsumerRecord[String, String]] = null

    val ranges = readOffSet()
    if (ranges == null){
      val topics = Array("test")
      ds = KafkaUtils.createDirectStream[String, String](scc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    } else {
      val topicTartitions = ranges.map(_.topicPartition())
      val offsets = ranges.map(x=>{x.topicPartition() -> x.untilOffset}).toMap
      ds = KafkaUtils.createDirectStream[String, String](scc, PreferConsistent, ConsumerStrategies.Assign[String, String](topicTartitions, kafkaParams, offsets))
    }

    ds.foreachRDD {rdd =>{
      rdd.map(x=>x.value).toDF("word").createOrReplaceTempView("test")
      spark.sql("select word, count(1) from test group by word").show()
      saveOffSet(rdd)
      //这个kafka必须是1.0+
      //ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
    }}

    scc.start()
    scc.awaitTermination()

  }


  def saveOffSet(rdd: RDD[_]): Unit ={
    val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetRanges: Array[String] = ranges.map(x=>{s"${x.topic},${x.partition},${x.fromOffset},${x.untilOffset}"})
    val str = StringUtils.join(offsetRanges.toList.asJava,"#@#")

    val client = new Jedis("192.168.240.131", 6379)
    client.set("PrdKafkaWordCount.offsets" ,str)
    client.close()
  }

  def readOffSet(): Array[OffsetRange] = {
    val client = new Jedis("192.168.240.131", 6379)
    val offsets = client.get("PrdKafkaWordCount.offsets")
    client.close()
    if (offsets == null){
      return null
    }
    val offsetRanges: Array[String] = offsets.split("#@#")
    offsetRanges.map(x=>{
      val offsetArr = x.split(",")
      val topicPartition = new TopicPartition(offsetArr(0),offsetArr(1).toInt)
      OffsetRange.create(topicPartition,offsetArr(2).toLong,offsetArr(3).toLong)})
  }
}

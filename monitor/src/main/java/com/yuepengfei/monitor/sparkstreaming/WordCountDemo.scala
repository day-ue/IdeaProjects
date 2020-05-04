package com.yuepengfei.monitor.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountDemo extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))

  val lines = ssc.socketTextStream("localhost", 8080)

  lines.print()

  ssc.start()
  ssc.awaitTermination()
}

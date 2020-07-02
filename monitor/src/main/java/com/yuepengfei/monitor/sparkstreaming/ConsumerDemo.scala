package com.yuepengfei.monitor.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random

object ConsumerDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkWordCountDemo").set("spark.executor.memory", "1G")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(2))
    //ssc.checkpoint("./data/checkpoint")
    val consumer = ssc.receiverStream(new Receiver[String](StorageLevel.MEMORY_ONLY) {
      override def onStart(): Unit = {
        print("自定义接收器开始接收数据")
        new Thread(new Runnable {
          override def run(): Unit = {
            val words = Array("spark", "flink", "elasticsearch", "hbase", "mysql")
            val random = new Random()
            while (true) {
              store(words(random.nextInt(5)))
              Thread.sleep(1000)
            }
          }
        }).start()
      }

      override def onStop(): Unit = {
        print("接收数据结束。。。。。。。。")
      }
    })

    consumer.foreachRDD(rdd =>{
      ssc.sparkContext.runJob(rdd,(iter:Iterator[String])=>{iter.foreach(println)})
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

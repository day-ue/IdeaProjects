package com.yuepengfei.monitor.structuredsteaming



import java.io.{File, FileWriter}

import com.yuepengfei.monitor.tool.RemoteShellExecutor
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Flume2Delta extends App {

  //启动kafka
/*  private val t1 = new Thread(new Runnable {
    override def run(): Unit = {
      val executor = Runtime.getRuntime
      executor.exec("/home/dayue/start/zkStart.py")
      Thread.sleep(30*1000)
      executor.exec("/home/dayue/start/kafkaStart.py")
      Thread.sleep(30*1000)
    }
  }).start()*/

  //启动flume
/*  private val t2 = new Thread(new Runnable {
    override def run(): Unit = {
      val executor = Runtime.getRuntime
      executor.exec("/home/dayue/app/apache-flume-1.9.0-bin/bin/flume-ng agent -c /home/dayue/app/apache-flume-1.9.0-bin/conf -f /home/dayue/app/apache-flume-1.9.0-bin/conf/my_test_log.conf -n pro -Dflume.root.logger=INFO,console")
    }
    Thread.sleep(50*1000)
  }).start()*/


  //启动spark任务
  private val t3 = new Thread(new Runnable {
    override def run(): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Flume2Delta")

      val spark = SparkSession.builder().config(sparkConf).getOrCreate()
      val readStream: DataFrame = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .load()

      import org.apache.spark.sql.functions._
      readStream
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select("value")
        .withColumn("word", split(col("value"),"\\|")(0))
        .withColumn("time", split(col("value"),"\\|")(1))
        .select("word","time")
        .writeStream
        .foreachBatch {
          (batchDF: DataFrame, batchId: Long) =>{
            // todo 这里如果不持久化会出问题吗? To avoid recomputations, you should cache the output DataFrame/Dataset, write it to multiple locations, and then uncache it.
            batchDF.persist()
            batchDF.show()
            //todo 还是不能像数据湖里写，会产生大量的小文件
            //batchDF.repartition(1).write.format("delta").mode("overwrite").save("./data/delta/delta-streaming")
            batchDF.unpersist()
          }
        }
        .trigger(Trigger.ProcessingTime("1 seconds"))
        .start()
        .awaitTermination()
    }
  }).start()

  //启动日志生成任务
  private val t4 = new Thread(new Runnable {
    override def run(): Unit = {
      val arr = Array("spark", "flink", "kafka", "hive", "elasticsearch", "redis", "java", "python", "mysql")
      val random = new Random()
      //val words = new ArrayBuffer[String]()
      //打开文件向里面写日志
      val file = new File("/home/dayue/data/log/stdout.log")
      if(!file.exists()) file.createNewFile()
      val writer = new FileWriter(file , true)
      try {
        while (true) {
          //words += arr(random.nextInt(9))
          val format = FastDateFormat.getInstance("yyyyMMdd HH:mm:ss")
          val log = s"${arr(random.nextInt(9))}|${format.format(System.currentTimeMillis())}"
          writer.write(log + "\r\n")
          writer.flush()
          Thread.sleep(1000)
        }
      } finally {
        writer.close()
      }
      //words.mkString("[", "," ,"]")
    }
  }).start()


}


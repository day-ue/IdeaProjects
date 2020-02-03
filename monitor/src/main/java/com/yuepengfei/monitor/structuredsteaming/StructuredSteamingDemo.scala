package com.yuepengfei.monitor.structuredsteaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.python.bouncycastle.jcajce.provider.digest.Tiger

/**
 * structuredSteaming现在只支持额kafka1.0，大部分公司使用的0.8，所以功能工作中暂时还用不到
 */
object StructuredSteamingDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StructuredSteamingDemo").set("spark.executor.memory", "1G")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //sql中使用的udf
    spark.udf.register("tt", ()=>{ System.currentTimeMillis() })
    //DF中使用的udf
    import org.apache.spark.sql.functions._
    val tt = udf(() => {
      new Timestamp(System.currentTimeMillis())
    })


    val query = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .withColumn("timestamp", tt())
      .withWatermark("timestamp","1 seconds")
      .groupBy(window(col("timestamp"), "10 seconds","5 seconds"), col("value"))
      .agg(count("timestamp"))
      .withColumnRenamed("value", "word")
      .withColumnRenamed("count(timestamp)", "count")
      .writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
  }
}

package com.yuepengfei.monitor.structuredsteaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * structuredSteaming现在只支持额kafka1.0，大部分公司使用的0.8，所以功能工作中暂时还用不到
 */
object StructuredSteamingDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StructuredSteamingDemo").set("spark.executor.memory", "1G")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val query = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.240.131:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

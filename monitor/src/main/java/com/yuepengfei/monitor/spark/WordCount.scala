package com.yuepengfei.monitor.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object WordCount extends App {
  private val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
  private val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

  val words = Array("spark", "flink", "elasticsearch", "kafka", "zepplin", "hadoop")
  private val random = new Random()
  private val data = new ArrayBuffer[String]()

  for (i <- 1 to 1000){
    data.append(words(random.nextInt(6)))
  }


  spark
    .sparkContext
    .parallelize(data)
    .map((_, 1))
    .reduceByKey(_+_)
    .foreach(x => println(x))


  spark.stop()
}

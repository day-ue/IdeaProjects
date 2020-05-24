package com.yuepengfei.monitor.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DeltaDemo extends App {

  private val arr = Array("spark", "flink", "hadoop", "kafka", "redis", "elasticsearch")
  private val words = new ArrayBuffer[String]()
  for (i <- 0 to 100){
    words += arr(Random.nextInt(6))
  }

  private val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DeltaDemo")
  private val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  import spark.implicits._
  val data = spark.sparkContext.parallelize(words).cache()
  data.toDF("word").createOrReplaceTempView("tmpTable")
  val result = spark.sql(
    """
      |select word, count(1) as num from tmpTable group by word
      |""".stripMargin)
  //写入数据
  result.repartition(1).write.format("delta").save("./data/delta/delta-table")
  //更新数据
  result.write.format("delta").mode("overwrite").save("./data/delta/delta-table")
  val df = spark.read.format("delta").load("./data/delta/delta-table")
  spark.read.format("delta").option("versionAsOf", 0).load("./data/delta/delta-table").show()
  spark.read.format("delta").option("versionAsOf", 1).load("./data/delta/delta-table").show()
  df.show()
  spark.close()

}

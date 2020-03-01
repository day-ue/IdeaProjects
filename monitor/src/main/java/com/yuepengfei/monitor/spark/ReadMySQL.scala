package com.yuepengfei.monitor.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ReadMySQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark read mysql")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val prop = new Properties()
    prop.put("user","dayue")
    prop.put("password","111111")
    val df = spark.read.jdbc("jdbc:mysql://127.0.0.1:3306/dayue_test", "student", prop)
    df.show()

    spark.stop()
  }

}

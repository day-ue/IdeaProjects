package com.yuepengfei.monitor.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkTrait {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)


  lazy private val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTrait")
  lazy val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

}

package com.yuepengfei.monitor.structuredsteaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object MysqlBinlog extends App {

  private val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MysqlBinlog")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val df = spark.readStream.
    format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
    option("host","localhost").
    option("port","3306").
    option("userName","root").
    option("password","102079").
    option("databaseNamePattern","yuepengfei").
    option("tableNamePattern","student").
    option("bingLogNamePrefix","student").
    option("binlogIndex","10").
    option("binlogFileOffset","90840").
    load()

  val query = df.writeStream.
    format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
    option("__path__","./data/spark-warehouse/yupengfei/student").
    option("path","yuepengfei/student").
    option("mode","Append").
    option("idCols","id").
    option("duration","3").
    option("syncType","binlog").
    option("checkpointLocation", "./data/spark/checkpoint/cpl-binlog2").
    outputMode("append")
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .start()

  query.awaitTermination()
}

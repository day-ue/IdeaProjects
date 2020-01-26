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
    option("host","192.168.240.131").
    option("port","3306").
    option("userName","root").
    option("password","123456").
    option("databaseNamePattern","test").
    option("tableNamePattern","mlsql_binlog").
    option("bingLogNamePrefix","mysql-bin").
    option("binlogIndex","10").
    option("binlogFileOffset","90840").
    load()

  val query = df.writeStream.
    format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
    option("__path__","/tmp/datahouse/{db}/{table}").
    option("path","{db}/{table}").
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

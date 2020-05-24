package com.yuepengfei.monitor.sparkstreaming

/**
 * 这个是自己随手写的demo，其中实现了通过rdd操作数据流，手动实现滚动窗口，控制offset
 */

import java.lang
import java.util.concurrent.{ArrayBlockingQueue, SynchronousQueue}

import com.yuepengfei.monitor.bean.RDDTime
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import scala.collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer

object SparkStreamingWordCountDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkWordCountDemo").set("spark.executor.memory", "1G")

    //实际开发中我们通常使用dataframe,所以必须创建sparksession，从而引入隐式转换
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val scc = new StreamingContext(spark.sparkContext, Seconds(2))
    scc.checkpoint("./data/checkpoint")

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics = Array("test")
    val ds = KafkaUtils.createDirectStream[String, String](scc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    //1. 常规的window函数完成计算
    executeWindow(ds)

    //遍历每个rdd对其进行操作
    //2. operation1RDD(spark, ds)

    //3. 遍历每一个rdd, 自己完成窗口操作
    //operation2RDD(spark, ds, kafkaParams)

    scc.start()
    scc.awaitTermination()
  }


  private def operation2RDD(spark: SparkSession, ds: InputDStream[ConsumerRecord[String, String]], kafkaParams: Map[String, Object]): Unit = {
    var countRDD: Long = 0L
    val ranges = new ArrayBlockingQueue[Array[OffsetRange]](3)
    ds.foreachRDD(rdd => {
      countRDD = countRDD + 1
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val size: Int = ranges.size()
      var head: Array[OffsetRange] = null
      if (size < 3) {
        ranges.put(offsetRanges)
        head = ranges.peek()
      } else {
        ranges.poll()
        ranges.put(offsetRanges)
        head = ranges.poll()
      }

      val ar = new ArrayBuffer[OffsetRange]()

      offsetRanges.foreach(o => {
        if (head == null) {
          ar += o
        } else {
          val headOffsetRange = head.filter(_.partition == o.partition).head
          val range: OffsetRange = OffsetRange(o.topic, o.partition, headOffsetRange.fromOffset, o.untilOffset)
          ar += range
        }
      })

      if (countRDD % 1 == 0) {
        val rdd = KafkaUtils.createRDD[String, String](spark.sparkContext, kafkaParams.asJava, ar.toArray, PreferConsistent)
        println(rdd.count())
        rdd.foreach(println)
      }

    })

  }

  private def operation1RDD(spark: SparkSession, ds: InputDStream[ConsumerRecord[String, String]])

  = {
    val accumulator: LongAccumulator = spark.sparkContext.longAccumulator("longAccumulator")
    val time = new RDDTime()
    ds.foreachRDD { rdd =>
      accumulator.add(1L)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"topic: ${o.topic}\npartition: ${o.partition}\nfromOffset: ${o.fromOffset}\nuntilOffset: ${o.untilOffset}")
      }
      time.add(System.currentTimeMillis())
      println(s"+++++++++++每个RDD生成时间间隔: ${(time.end - time.start)}秒++++++++++第${accumulator.value}个RDD")
    }
  }

  private def executeWindow(ds: InputDStream[ConsumerRecord[String, String]])

  = {
    ds.map(record => record.value())
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(40), Seconds(20))
      .print()
  }
}

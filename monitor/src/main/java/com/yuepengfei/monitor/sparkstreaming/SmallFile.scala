package com.yuepengfei.monitor.sparkstreaming

import java.io.{File, FileWriter}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SmallFile {
  def main(args: Array[String]) {

    val ssc = StreamingContext.getOrCreate("./data/checkpoint/SmallFile",
      () => createContext("./data/checkpoint/SmallFile"))
    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(str: String): StreamingContext ={
    // 创建一个批处理时间是2s的context 要增加环境变量
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(this.getClass.getName).setMaster("local[*]")
    sparkConf.registerKryoClasses(Array(
      classOf[Array[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]]
    ))
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //ssc.checkpoint("./data/checkpoint/SmallFile")
    // 使用broker和topic创建DirectStream
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SmallFile",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val messages = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaParams))
    // 指定(partitionid,path)
    val files = Map(0 -> "D:\\code\\day_ue\\IdeaProjects\\monitor\\data\\1.txt")
    val lines = messages.map(_.value)
    lines.foreachRDD(rdd => {
      // 生成内容是(PartitionId,Path)的广播变量
      val bc_files = ssc.sparkContext.broadcast(files);
      // 自定义写函数
      val func = (itr: Iterator[String]) => {
        //        if (itr.nonEmpty) {
        val id = TaskContext.getPartitionId()
        val path = bc_files.value.get(id);
        println("=======id " + id)
        append2File(path.get, itr)
        (id, path)
        //        }
      }
      // 重分区RDD，并运行sc.runjob
      val rep: RDD[String] = rdd.repartition(1)
      val res: Array[Any] = ssc.sparkContext.runJob(rep, func)
      // 判断文件是否超过大戏，进行重命名和新建。
      res.foreach { case (id: Int, fileName: Option[String]) => {
        if (fileName.get.length > 1)
          if (getFileSize(fileName.get) > 1024 * 1024 * 1) {
            renameAndCreateFile(fileName.get)
          }
      }
      }
      // 销毁广播变量
      //bc_files.unpersist()
      bc_files.destroy()
    })
    ssc
  }

  // 重命名文件及重新创建文件
  def renameAndCreateFile(fileName: String) {
    val file = new File(fileName)
    file.renameTo(new File(fileName + "_" + System.currentTimeMillis() + "_tmp"))
    val newFile = new File(fileName)
    if (!newFile.exists()) {
      newFile.createNewFile()
    }
  }

  // 获取文件的大小
  def getFileSize(fileName: String): Long = {
    new File(fileName).length()
  }

  // 往文件里追加数据
  def append2File(fileName: String, itr: Iterator[String]) {
    //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
    try {
      val writer = new FileWriter(fileName, true);
      itr.foreach(line => {
        writer.write(line + "\n");
      })
      writer.close();
    } catch {
      case e: ArithmeticException => println(e)
    }
  }
}
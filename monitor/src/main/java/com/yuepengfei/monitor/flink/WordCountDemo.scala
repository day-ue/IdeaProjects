package com.yuepengfei.monitor.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("./data/word.txt")

    val result: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    result.writeAsText("./data/wordCountOut").setParallelism(2)

    env.execute("my first task")
  }
}

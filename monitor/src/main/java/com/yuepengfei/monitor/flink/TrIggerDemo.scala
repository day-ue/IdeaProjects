package com.yuepengfei.monitor.flink

import java.lang
import java.util.{Map, Random}

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.windowing.{ProcessWindowFunction, RichProcessWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ProcessingTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._


/**
 * 需求：每5min计算今天的用户量。
 * 开1天的滚动窗口，5min触发一次计算
 */

//todo 未完成

object TriggerDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建主流
    val dataStreamSource: DataStream[String] = env.addSource(new RichParallelSourceFunction[String] {
      var isRun: Boolean = _

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val words = Array("spark", "suning", "spring", "flink", "kafka", "hadoop")
        //这个流产生1000个单词就结束了
        for (i <- 1 to 1000) {
          val random = new Random
          val message = "主流中的单词是"+words(random.nextInt(6))
          println(message)
          ctx.collect(message)
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {
        isRun = false
      }
    })

    dataStreamSource.map((_,1)).setParallelism(1)
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .trigger(new MyTrigger)
      .process(new MyProcess())
      //.reduce((x,y)=>(x._1 , x._2 + y._2)).setParallelism(1)
      .print("keyword")

    env.execute("triggerDemo")

  }
}

class MyProcess extends RichProcessWindowFunction[Int, Int, String, TimeWindow]{
  /** process function维持的状态  */
  lazy val state: MapState[String, Int] = getRuntimeContext
    .getMapState(new MapStateDescriptor[String, Int]("myState",classOf[String], classOf[Int]))
  override def process(key: String, context: ProcessWindowFunction[Int, Int, String, TimeWindow]#Context, elements: lang.Iterable[Int], out: Collector[Int]): Unit = {
    elements.asScala.foreach(x =>{
      if(state.contains(key)){
        state.put(key, state.get(key) + x)
      } else{
        state.put(key, x)
      }
    })
    state.entries.asScala.foreach(x => {
      out.collect((x.getValue))
    })

  }
}



/**
 * 每来一条数据计算一次，一分钟清理一次状态
 */
class MyTrigger extends Trigger[Any, TimeWindow]{

  var count :Int= 0

  override def onElement(element: Any, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
    {
      count = count + 1
      if(count%10 == 0){
        println("----------------------------每10个消息触发一次计算-------------------------------")
        return TriggerResult.FIRE
      }else{
        ctx.registerProcessingTimeTimer(window.maxTimestamp)
        return TriggerResult.CONTINUE
      }
    }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("--------------------------------一个滚动窗口触发一次计算-------------------------------------")
    TriggerResult.FIRE_AND_PURGE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = return TriggerResult.CONTINUE

  @throws[Exception]
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteProcessingTimeTimer(window.maxTimestamp)
    println("--------------------------------------清理窗口数据----------------------------------------------------")
  }

  override def canMerge = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = { // only register a timer if the time is not yet past the end of the merged window
    // this is in line with the logic in onElement(). If the time is past the end of
    // the window onElement() will fire and setting a timer here would fire the window twice.
    val windowMaxTimestamp = window.maxTimestamp
    if (windowMaxTimestamp > ctx.getCurrentProcessingTime) ctx.registerProcessingTimeTimer(windowMaxTimestamp)
  }

  override def toString = "YUEPENGFEI"
}
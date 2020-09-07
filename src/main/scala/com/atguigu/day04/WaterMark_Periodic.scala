package com.atguigu.day04

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMark_Periodic {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置wk的生成周期
    env.getConfig.setAutoWatermarkInterval(3000L)

    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    val reusltDS: DataStream[String] = socketDS
      .map(
        line => {
          val words: Array[String] = line.split(",")
          WaterSensor(words(0), words(1).toLong, words(2).toDouble)
        }
      )
      .assignTimestampsAndWatermarks(
        new AssignerWithPeriodicWatermarks[WaterSensor] {
          //创建一个变量，用来存储当前最大的时间戳
          var currentTs: Long = 0L

          //获取wm的方法
          override def getCurrentWatermark: Watermark = {
            println("生成CurrentWatermark...")
            //根据当前最大的时间戳
            new Watermark(currentTs)

          }

          //获取eventime的方法
          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
            //更新存储的最大时间戳,两者比大小
            currentTs = currentTs.max(element.ts * 1000L)
            //返回数据中的时间字段，作为事件事件
            element.ts * 1000L
          }
        }
      )
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(
        new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
          //输出上下文相关的信息
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            out.collect(
              "当前key=" + key
                + ",属于的窗口[" + context.window.getStart + "," + context.window.getEnd
                + "],一共有" + elements.size + "条数据"
                + ",当前是WaterMark=" + context.currentWatermark
            )
          }
        }
      )

    reusltDS.print("WaterMark_Periodic")

    env.execute()

  }

}

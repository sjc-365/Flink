package com.atguigu.day04

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WarterMark_WindowType {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)


    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    val sideoutputDS: DataStream[WaterSensor] = socketDS
      .map(
        line => {
          val words: Array[String] = line.split(",")
          WaterSensor(words(0), words(1).toLong, words(2).toDouble)
        }
      )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
          override def extractTimestamp(element: WaterSensor): Long = {
            element.ts * 1000L
          }
        }
      )


    val resultDS: DataStream[(String, Int)] = sideoutputDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      //.window(SlidingProcessingTimeWindows)
      //事件事件滑动窗口
      //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //事件事件滚动窗口
      //.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(3)))
      //事件事件会话窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(3)))
      .sum(1)




    resultDS.print("WindowType")

    env.execute()
  }

}

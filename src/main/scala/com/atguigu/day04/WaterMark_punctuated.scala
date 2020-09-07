package com.atguigu.day04

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

//wm的生成时间控制
object WaterMark_punctuated {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    socketDS
      .map(
        line => {
          val words: Array[String] = line.split(",")
          WaterSensor(words(0), words(1).toLong, words(2).toDouble)
        }
      )
      .assignTimestampsAndWatermarks(
        new AssignerWithPunctuatedWatermarks[WaterSensor] {
          //检查和获取wm的方法
          override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
            println("生成checkAndGetNextWatermark...")
            //直接使用
            new Watermark(extractedTimestamp)

          }
          //抽取字段作为事件时间
          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
            element.ts * 1000L
          }
        }
      )
      .map(data => (data.id,1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print("WaterMark_punctuated")

    env.execute()
  }

}

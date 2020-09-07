package com.atguigu.day04

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMark_SideOutPut {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    val outputTag: OutputTag[(String, Int)] = new OutputTag[(String,Int)]("late data")

    val resultDS: DataStream[String] = sideoutputDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(outputTag) //侧输出流的标签
      .process(
        new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            out.collect(
              "当前的key是=" + key
                + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
                + "]，数据=" + elements.toString()
                + "，当前的watermark=" + context.currentWatermark
            )
          }
        }
      )

    //从主流中获取侧输出流的数据
    resultDS.getSideOutput(outputTag).print("sideoutput")

    resultDS.print("late data")

    env.execute()
  }

}

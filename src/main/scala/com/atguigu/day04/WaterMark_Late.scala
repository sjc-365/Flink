package com.atguigu.day04

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object WaterMark_Late {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置时间语义--仍使用默认的时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val socketDS: DataStream[WaterSensor] = env.
      socketTextStream("localhost", 9999)
      .map(log => {
        val words: Array[String] = log.split(",")
        WaterSensor(words(0), words(1).toLong, words(2).toDouble)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
          override def extractTimestamp(t: WaterSensor): Long = {
            t.ts * 1000L
          }
        }
      )


    val resultDS: DataStream[String] = socketDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
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

    resultDS.print("Late_data")

    env.execute()
  }
}

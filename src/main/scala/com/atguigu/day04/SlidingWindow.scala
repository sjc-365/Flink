package com.atguigu.day04

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


//滑步窗口
object SlidingWindow {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    //
    val resultWS: WindowedStream[(String, Int), Tuple, TimeWindow] = socketDS.map((_, 1)).keyBy(0).timeWindow(Time.seconds(5), Time.seconds(1))

    resultWS.sum(1).print("滑步窗口：")

    env.execute()

  }
}

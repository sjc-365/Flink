package com.atguigu.day04

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object SessionWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val keydeDS: KeyedStream[(String, Int), Tuple] = env.socketTextStream("localhost", 9999).map((_, 1)).keyBy(0)

    //会话窗口，数据之间间隔超过3秒，则关闭当前窗口，开启上一个窗口
    val dataWS: WindowedStream[(String, Int), Tuple, TimeWindow] = keydeDS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))

    dataWS.sum(1).print("aaa")

    env.execute()
  }

}

package com.atguigu.day04


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


//滑动窗口
object TumblingWindow {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    //读取数据源
    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    //将数据转为map结构并分组开窗
    val result: DataStream[(String, Int)] = socketDS.map((_, 1)).keyBy(0).timeWindow(Time.seconds(8)).sum(1)

    result.print("TimeWindows:")

    env.execute()

  }

}

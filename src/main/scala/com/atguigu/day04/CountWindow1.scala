package com.atguigu.day04

import org.apache.flink.streaming.api.scala._

object CountWindow1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    val resultDS: DataStream[(String, Int)] = socketDS.map((_, 1)).keyBy(0).countWindow(5).sum(1)

    resultDS.print("bbb")

    env.execute()
  }

}

package com.atguigu.day01

import org.apache.flink.streaming.api.scala._

object UnboundStreamWordCount {

  def main(args: Array[String]): Unit = {
    //环境准备
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //监听端口，获取数据
    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    //处理数据
    val result: DataStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    //打印数据
    result.print()

    //启动流
    env.execute()
  }

}

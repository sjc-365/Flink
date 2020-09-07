package com.atguigu.day01

import org.apache.flink.streaming.api.scala._

object BoundStreamWordCount {

  def main(args: Array[String]): Unit = {
    //环境准备
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //监听端口，获取数据
    val socketDS: DataStream[String] = env.readTextFile("/home/atguigu/input/word.txt")

    //处理数据
    val result: DataStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    //打印数据
    result.print()

    //启动流
    env.execute()
  }
}

package com.atguigu.day03

import org.apache.flink.streaming.api.scala._

object PV {

  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.获取数据
    val fileDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")

    //转换为样例类
    val UserDS: DataStream[UserBehavior] = fileDS.map(line => {
      val words: Array[String] = line.split(",")
      UserBehavior(
        words(0).toLong,
        words(1).toLong,
        words(2).toInt,
        words(3),
        words(4).toLong
      )
    })

    //过滤数据
    val pvDS: DataStream[UserBehavior] = UserDS.filter(user => user.behavior == "pv")

    //转换为map结构（pv,1)，key是同一个，value均3是1，最后求和
    val result: DataStream[(String, Int)] = pvDS.map(data => ("pv", 1)).keyBy(0).sum(1)

    result.print()

    env.execute()

  }
case class  UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
}



package com.atguigu.day06

import com.atguigu.day06.HotItemAnalysis.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//需求：实时统计每小时内的网站PV
object Pv{
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceDS: DataStream[UserBehavior] = env
      .readTextFile("input/UserBehavior.csv")
      .map(
        line => {
          val words: Array[String] = line.split(",")
          UserBehavior(
            words(0).toLong,
            words(1).toLong,
            words(2).toInt,
            words(3),
            words(4).toLong)
        }
      )
      //设置时间抽取和WaterMark的生成
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //开窗统计浏览量
    val resultDS: DataStream[(String, Int)] = sourceDS
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindowAll(Time.hours(1))
      .sum(1)

    resultDS.print("一小时内网站的浏览统计：")

    env.execute()
  }



}

package com.atguigu.day06

import com.atguigu.day06.HotItemAnalysis.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

//需求：实时统计每小时内的网站UV
object Uv{
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
    sourceDS
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(data => data._1)
      .timeWindow(Time.hours(1))
      .process(
        new ProcessWindowFunction[(String,Long),Long, String, TimeWindow] {
          //定义一个set用来存储用户id.set集合实现去重
          val uvCount: mutable.Set[Long] = mutable.Set[Long]()

          override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[Long]): Unit = {
            for (elem <- elements) {
              uvCount.add(elem._2)
            }
            //输出id数量
            out.collect(uvCount.size)
            uvCount.clear()
          }

        }
      )
      .print("一小时内网站的uv浏览统计：")

    env.execute()
  }



}

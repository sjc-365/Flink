package com.atguigu.day04

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//使用默认的时间语义进行数据处理（处理时间）

object EventTime {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置时间语义--仍使用默认的时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val result: DataStream[(String, Int)] = env.
      socketTextStream("localhost", 9999)
      .map(log => {
        val words: Array[String] = log.split(",")
        WaterSensor(words(0), words(1).toLong, words(2).toDouble)
      })
      .assignAscendingTimestamps(_.ts*1000L)
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sum(1)

    result.print("ProcessingTime")

    env.execute()
  }

}

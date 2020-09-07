package com.atguigu.day07

import com.atguigu.day06.LoginDetect.LoginEvent
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginDetectWithCEP {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceDS: DataStream[LoginEvent] = env
      .readTextFile("input/LoginLog.csv")
      .map(
        line => {
          val words: Array[String] = line.split(",")
          LoginEvent(
            words(0).toLong,
            words(1),
            words(2),
            words(3).toLong)
        }
      )
      //设置时间抽取和WaterMark的生成
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
          override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
        }
      )

    //处理数据 2秒钟内连续两次登录失败
    //1.分组
    val keyDS: KeyedStream[LoginEvent, Long] = sourceDS.keyBy(_.userId)

    //2.制定CEP规则
    val loginPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("start")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    //2.应用规则
    val patternPS: PatternStream[LoginEvent] = CEP.pattern(keyDS, loginPattern)

    //3.获取匹配的结果
    val result: DataStream[String] = patternPS.select(data => data.toString())

    result.print("result")

    env.execute()
  }
}

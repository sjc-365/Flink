package com.atguigu.day07

import java.sql.Timestamp

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeOutWithCEP2 {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceDS: DataStream[OrderEvent] = env
      .readTextFile("input/OrderLog.csv")
      .map(
        line => {
          val words: Array[String] = line.split(",")
          OrderEvent(
            words(0).toLong,
            words(1),
            words(2),
            words(3).toLong)
        }
      )
      //设置时间抽取和WaterMark的生成
      .assignAscendingTimestamps(_.eventTime * 1000L)

    //处理数据
    //1.分组
    val keyDS: KeyedStream[OrderEvent, Long] = sourceDS.keyBy(_.orderId)

    //2.制定CEP规则
    val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //3.应用规则
    val orderPS: PatternStream[OrderEvent] = CEP.pattern(keyDS, orderPattern)

    //4.获取匹配规则的数据
    // 3.2 使用CEP实现超时监控
    // 局限性： 1. 没法发现，超时，但是支付的情况，但是一般这种情况，本身就是一种异常的情况
    //         2. 没法发现，create数据丢失的情况，但是这种情况，本身就是异常情况


    val timeoutTag = new OutputTag[String]("timeout")
    //超时数据放在侧输出流，从侧输出流拿出
    //参数1：侧输出流标签对象
    //参数2：流中的超时数据放到侧输出流中
    //参数3：对匹配上的数据进行处理
    val resultDS: DataStream[String] = orderPS.select(timeoutTag)((timeoutData, ts) => {
      timeoutData.toString()
    }
    )(data => data.toString())

    resultDS.getSideOutput(timeoutTag)print()

    env.execute()
  }

  case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
}

package com.atguigu.day07

import java.sql.Timestamp

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeOutWithCEP {
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

    //4.获取匹配的结果
    orderPS.select(
      data => {
        //获取正常的下单、支付数据
        /*val createEvent: OrderEvent = data("create").iterator.next()
        val payEvent: OrderEvent = data("pay").iterator.next()*/

        val createEvent: OrderEvent = data("create").iterator.next()
        val payEvent: OrderEvent = data("pay").iterator.next()

        s"""
          |订单ID:${new Timestamp(createEvent.orderId)}
          |创建时间:${new Timestamp(createEvent.eventTime)}
          |支付时间:${new Timestamp(payEvent.eventTime)}
          |耗时:${new Timestamp(payEvent.eventTime-createEvent.eventTime)}
          |-------------------------------------------
          |""".stripMargin

      }
    )
      .print("对账成功数据")


    env.execute()
  }

  case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
}

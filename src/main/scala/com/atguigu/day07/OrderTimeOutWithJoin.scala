package com.atguigu.day07

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//双流join操作，仅支持事件时间
object OrderTimeOutWithJoin {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //订单流
    val orderDS: DataStream[OrderEvent] = env
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

    //支付流
    val txDS: DataStream[TxEvent] = env
      .readTextFile("input/ReceiptLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          TxEvent(
            datas(0),
            datas(1),
            datas(2).toLong
          )
        }
      )
      .assignAscendingTimestamps(_.eventTime * 1000L)

    //对双流分别进行分组
    val orderKyeDS: KeyedStream[OrderEvent, String] = orderDS.keyBy(_.txId)
    val txKeyDS: KeyedStream[TxEvent, String] = txDS.keyBy(_.txId)

    //双流join，并设置时间跨度
    val result: DataStream[String] = orderKyeDS
      .intervalJoin(txKeyDS)
      //辐射的时间跨度为10s
      .between(Time.seconds(-5), Time.seconds(5))
      //对数据进行处理
      .process(
        new ProcessJoinFunction[OrderEvent, TxEvent, String] {
          override def processElement(left: OrderEvent, right: TxEvent, ctx: ProcessJoinFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
            //如果两个数据匹配上了
            if (left.txId == right.txId) {
              out.collect(s"""
                 |订单匹配上了，
                 |订单id：${left.txId}
                 |ordertime:${new Timestamp(left.eventTime)}
                 |ordertime:${new Timestamp(right.eventTime)}
                 |""".stripMargin)
            }
          }
        }
      )

    result.print("result")

    env.execute()
  }

  case class TxEvent(txId: String, payChannel: String, eventTime: Long)

  case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
}

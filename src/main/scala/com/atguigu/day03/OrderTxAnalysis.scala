package com.atguigu.day03

import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object OrderTxAnalysis {

  def main(args: Array[String]): Unit = {
    //获取两个流的数据 订单流、支付流
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //订单数据
    val OrderDS = env
      .readTextFile("input/OrderLog.csv")
      .map(line => {
        val words = line.split(",")
        OrderEvent(words(0).toLong, words(1), words(2), words(3).toLong)
      })

    //支付数据
    val PaymentDS = env
      .readTextFile("input/ReceiptLog.csv")
      .map(line => {
        val words = line.split(",")
        TxEvent(words(0), words(1), words(2).toLong)
      })

    //将数据分组
    val OrderMap = OrderDS.keyBy(_.txId)
    val PaymentMap = PaymentDS.keyBy(_.txId)

    ////双流jion操作
    val doubleDS = OrderMap.connect(PaymentMap)

    //调用process方法，对数据进行处理
    doubleDS.process(new MyProcess()).print("order tx")

    env.execute()


  }

  class MyProcess extends CoProcessFunction[OrderEvent, TxEvent, String] {
    //用来保存交易数据,key是交易码，value是完整数据
    val orderMap = new mutable.HashMap[String, OrderEvent]()
    //用来保存订单数据,key是交易码，value是完整数据
    val paymentMap = new mutable.HashMap[String, TxEvent]()

    //处理订单系统的数据
    override def processElement1(in1: OrderEvent, context: CoProcessFunction[OrderEvent, TxEvent, String]#Context, collector: Collector[String]): Unit = {
      //尝试将进来的数据从paymentmap中取出来
      val maybeEvent = paymentMap.get(in1.txId)

      //如果不存在，则保存到ordermap中
      if(maybeEvent.isEmpty){
        orderMap.put(in1.txId,in1)
      }else{
        //如果存在在，表示可以匹配到
        collector.collect("订单["+in1.orderId+"]对账成功")
        //输出，删除
        paymentMap.remove(in1.txId)
      }

  }
    //处理支付系统的数据
    override def processElement2(in2: TxEvent, context: CoProcessFunction[OrderEvent, TxEvent, String]#Context, collector: Collector[String]): Unit = {
      //尝试将进来的数据从paymentmap中取出来
      val maybeEvent = orderMap.get(in2.txId)

      //如果不存在，则保存到ordermap中
      if(maybeEvent.isEmpty){
        paymentMap.put(in2.txId,in2)
      }else{
        //如果存在在，表示可以匹配到
        collector.collect("订单["+maybeEvent.get.orderId+"]对账成功")
        //输出，删除
        orderMap.remove(in2.txId)
      }


}
}
  case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
  case class TxEvent( txId: String, payChannel: String, eventTime: Long )
}

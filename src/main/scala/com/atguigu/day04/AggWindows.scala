package com.atguigu.day04

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object AggWindows {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    //设置数量窗口，每收到同组数据2条就开始计算一次
    val aggresult: WindowedStream[(String, Int), Tuple, TimeWindow] = socketDS.map((_, 1)).keyBy(0).timeWindow(Time.seconds(5), Time.seconds(2))

    //reduce--增量聚合函数1
    /*aggresult.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
        (t1._1,t._2+t1._2)
      }
    }).print()*/

    //aggregate--增量聚合函数2
   /* aggresult.aggregate(
      new AggregateFunction[(String,Int),Long,(String,Long)]{
        //初始化累计器
        override def createAccumulator(): Long = 0L
        //数据运算逻辑
        override def add(in: (String, Int), acc: Long): Long = in._2 + acc
        //返回结果
        override def getResult(acc: Long): (String, Long) = ("y",acc)
        //分区间数据合并
        override def merge(acc: Long, acc1: Long): Long = acc + acc1


    }).print()*/

    env.execute()

  }

}

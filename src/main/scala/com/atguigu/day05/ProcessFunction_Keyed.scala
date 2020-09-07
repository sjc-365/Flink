package com.atguigu.day05

import java.sql.Timestamp

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunction_Keyed {

  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据,转换成样例类
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost", 9999)
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })
      .assignAscendingTimestamps(_.ts * 1000L)

    //对数据进行分组开窗聚合

    val resultDS: DataStream[String] = socketDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .process(
        new KeyedProcessFunction[String, (String, Int), String] {
          //对数据处理的函数
          //在其中可以注册定时器
          override def processElement(value: (String, Int), ctx: KeyedProcessFunction[String, (String, Int), String]#Context, out: Collector[String]): Unit = {
            //输出一条信息
            println("注册定时器，注册时间为 = " + new Timestamp(ctx.timerService().currentProcessingTime()))
            println("注册定时器，注册时的WaterMark时间为 = " + ctx.timerService().currentWatermark())
            println("注册定时器，注册时的数据时间为 = " + new Timestamp(ctx.timestamp()))
            //基于处理时间注册定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L)
          }

          //触发定时器后。执行的操作在该函数中进行
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Int), String]#OnTimerContext, out: Collector[String]): Unit = {
            //输出一条信息
            //自己获取当前时间
            println("定时器触发了，触发时间 = " + new Timestamp(ctx.timerService().currentProcessingTime()))
            //timestamp为当前时间，flink已将该时间提供给开发者了
            println("定时器触发了，触发时间 = " + new Timestamp(timestamp))
            //查看warteMark,warteMark不会随着设备时间发生改变。是当前组的WM
            println("定时器触发了，warteMark = " + new Timestamp(ctx.timerService().currentWatermark()))
          }

        }
      )


    env.execute()

  }

}

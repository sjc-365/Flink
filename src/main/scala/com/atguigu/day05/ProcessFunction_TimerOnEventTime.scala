package com.atguigu.day05

import java.sql.Timestamp

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunction_TimerOnEventTime {

  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义
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
    //使用升序

    //对数据进行分组开窗聚合

    socketDS
      .keyBy(_.id)
      .process(
        new KeyedProcessFunction[String, WaterSensor, String] {
          var timeTS: Long = 0L

          //对数据处理的函数，在其中可以注册定时器
          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
           //只注册一个定时器，到点之后触发相关操作，不关心数据如何
            if(timeTS == 0){
              timeTS = value.ts * 1000L + 1000L
              ctx.timerService().registerEventTimeTimer(timeTS)
              println("注册定时器,注册时间是：" + new Timestamp(timeTS))
            }
          }

          //触发定时器后，执行的操作在该函数中定义
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
            println("当前时间是 ： " + new Timestamp(timestamp))
            println("5秒到了，触发定时器，输出......")
          }

        }
      )
      .print("aaa")


    env.execute()

  }

}

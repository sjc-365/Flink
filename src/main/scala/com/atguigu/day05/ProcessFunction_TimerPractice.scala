package com.atguigu.day05

import java.sql.Timestamp

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector


/**
 * 需求：监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警。
 * 处理思路：
 *  1.不考虑乱序的情况下，将数据的ts作为WM，通过wm开控制窗口的开关
 *  2.使用间隔类来设置WM的生成（每来一条数据，生成一个WM），保证告警信息及时推送
 *  3.分组之后，在process中对数据进行处理
 *  4.对数据进行处理：第一条数据来的时候，就注册一个5s的定时器，然后对比水位数据，如果新数据 > 旧数据，则保存新数据
 *      如果新数据 < 旧数据,则更新水位数据和重新注册定时器
 *
 */
object ProcessFunction_TimerPractice {

  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    //转换数据类型 设置WarteMark为间隔性生成wm
    socketDS
      .map(data => {
        val words: Array[String] = data.split(",")
        WaterSensor(words(0),words(1).toLong,words(2).toDouble)
      })
      .assignTimestampsAndWatermarks(
        new AssignerWithPunctuatedWatermarks[WaterSensor] {
          //获取wm
          override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
            new Watermark(extractedTimestamp)
          }

          //指定数据中哪个字段作为WM
          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
            element.ts * 1000L
          }
        }
      )
      .keyBy(_.id)
      .process(
        new KeyedProcessFunction[String,WaterSensor,String] {
          //水位信息
          var vcLength:Double = 0.0

          var timerTs: Long = 0L

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
           /* if(vcLength == 0.0) {
              //更新数据
              vcLength = value.vc
              timerTs = value.ts*1000L + 5000L
              //注册5秒的定时器
              ctx.timerService().registerEventTimeTimer(timerTs)
            } else{
              //如果新数据 < 旧数据,重新注册和清空数据
              if(value.vc < vcLength){
                ctx.timerService().deleteEventTimeTimer(timerTs)
                timerTs=0L
                vcLength=value.vc
              }else{
                vcLength=value.vc
              }
            }*/

            // 1.判断水位是否上涨
            if (value.vc >= vcLength) {
              // 1.1 水位上涨
              // 第一条数据来的时候，先注册一个 5s的定时器
              if (timerTs == 0) {
                // 保存定时器的时间
                timerTs = value.ts * 1000L + 5000L
                ctx.timerService().registerEventTimeTimer(timerTs)
              }
              // 更新保存的水位值
              vcLength = value.vc
            } else {
              // 1.2 水位下降:
              // 1.2.1、删除原来的定时器；
              ctx.timerService().deleteEventTimeTimer(timerTs)
              // 2、重新注册定时器（把定时器清零）;
              timerTs = 0L
              // 3、更新当前水位值
              vcLength = value.vc
            }
          }

          //定时器触发后进行的操作
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
            out.collect("在"+new Timestamp(timestamp)+"时发现，水位已连续5秒持续上涨")
            //触发之后，将时间归零
            timerTs = 0L
          }
        }
      )
      .print("timer")

    env.execute()
  }

}

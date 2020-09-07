package com.atguigu.day05



import java.sql.Timestamp

import com.atguigu.day02.WaterSensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector


//实现监控水位连续5s内上涨，则告警
//使用state如何实现：
//获取state,通过判断state的数值来输出报警信息
//还需要通过定时器，来确定5秒中的范围
//为什么不通过开窗来处理：开窗的话就没有定时器，且滚动窗口不合适，可能存在跨区域的5秒上涨
//滑动窗口同样不合适，无法确定数据是否每隔一秒来一条数据，如果在一秒中来了三条数据，分别是升降升三种情况，那么一秒的步长显然就不合适

object State_KeyedPractice {

  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据.乱序数据，设置了1秒的等待时间
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost", 9999)
      .map(data => {
        val words: Array[String] = data.split(",")
        WaterSensor(words(0), words(1).toLong, words(2).toDouble)
      })
      .assignTimestampsAndWatermarks(
        new AssignerWithPunctuatedWatermarks[WaterSensor]{
          override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
            new Watermark(extractedTimestamp)

          }
          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
            element.ts*1000L

          }
        }
      )


    //转换数据类型 设置WarteMark为间隔性生成wm
    val resultDS: DataStream[String] = socketDS
      .keyBy(_.id)
      .process(
        //实现监控水位连续5s内上涨，则告警
        new KeyedProcessFunction[String, WaterSensor, String] {
          //初始化state
          var CuttrentHeight: ValueState[Int] = _
          // 需要记录定时器时间
          var timerTs: ValueState[Long] = _

          //获取状态值
          override def open(parameters: Configuration): Unit = {
            CuttrentHeight = getRuntimeContext.getState(new ValueStateDescriptor[Int]("height", classOf[Int]))
            timerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))
          }

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
            println("当前的key=" + ctx.getCurrentKey + ",state保存的水位值=" + CuttrentHeight.value())

            //1.判断水位是否上涨
            if (value.vc >= CuttrentHeight.value()) {
              //判断是否为第一条数据/下降后的第一条数据
              if (timerTs.value() == 0) {
                //保存定时器的时间
                timerTs.update(value.ts * 1000L + 5000L)
                //注册定时器
                ctx.timerService().registerEventTimeTimer(timerTs.value())
              }
              //保存最新的水位数据
              CuttrentHeight.update(value.vc.toInt)
            } else {
              //水位下降
              //删除定时器
              ctx.timerService().deleteEventTimeTimer(timerTs.value())
              //更新时间
              timerTs.clear()
              //更新最新的水位数据
              CuttrentHeight.update(value.vc.toInt)
            }
          }

          //timestamp是触发器触发的时间
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
            //触发定时器后，需要执行的操作
            out.collect("在" + new Timestamp(timestamp) + "监测到连续 5s水位上涨")

            // 定时器触发后，清空保存的时间，避免影响后续的告警
            timerTs.clear()
          }

        }
      )

    resultDS.print("状态连续")

    env.execute()

  }
}

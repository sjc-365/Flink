package com.atguigu.day05

import java.sql.Timestamp

import com.atguigu.day02.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector


/**
 * 需求：采集监控传感器水位值，将水位值高于5cm的值输出到side output
 *  1.使用侧输出流，将警告信息传输给侧输出流，不影响正常数据
 *
 */
object ProcessFunction_TimerPractice_SideOutput {

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
    val resultDS: DataStream[WaterSensor] = socketDS
      .map(data => {
        val words: Array[String] = data.split(",")
        WaterSensor(words(0), words(1).toLong, words(2).toDouble)
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
        new KeyedProcessFunction[String, WaterSensor, WaterSensor] {
          //水位信息
          var vcLength: Double = 0.0
          var timerTs: Long = 0L

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, WaterSensor]#Context, out: Collector[WaterSensor]): Unit = {
            if (value.vc > 5) {
              //创建侧输出流
              val outputTag = new OutputTag[String]("high water level")
              ctx.output(outputTag, "水位超过阈值" + value.toString)
              //将报警数据也同样传递给下游
              out.collect(value)
            } else {
              //没超过则将数据传递给下游
              out.collect(value)
            }

          }
        }
      )


    //重新获取侧输出流，重点是String需要前后一致
    val outputTag = new OutputTag[String]("high water level")
    //打印侧输入流
    resultDS.getSideOutput(outputTag).print("alarm")
    //打印正常流
    resultDS.print("normal")

    env.execute()
  }

}

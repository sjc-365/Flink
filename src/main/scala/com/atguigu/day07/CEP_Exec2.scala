package com.atguigu.day07

import com.atguigu.day02.WaterSensor
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
object CEP_Exec2 {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sensorDS: DataStream[WaterSensor] = env
      .readTextFile("input/sensor-data-cep.log")
      .map(
        data => {
          val datas: Array[String] = data.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        }
      )
      .assignAscendingTimestamps(_.ts * 1000L)


    //CEP  定义规则--应用规则--获取匹配结果
    val pattern2: Pattern[WaterSensor, WaterSensor] = Pattern
      .begin[WaterSensor]("start")
      .where(_.id == "sensor_1")
        .oneOrMore.until(_.vc > 30)

    CEP.
      pattern(sensorDS,pattern2)
      .select(data => data.toString())
      .print("cep2")

    env.execute()

  }

}

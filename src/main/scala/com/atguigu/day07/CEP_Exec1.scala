package com.atguigu.day07

import com.atguigu.day02.WaterSensor
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._


object CEP_Exec1 {

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

    //TODO
    //1.定义规则，对数据进行规则过滤处理
    //
    val pattern1: Pattern[WaterSensor, WaterSensor] = Pattern
      .begin[WaterSensor]("begin")
      .where(_.id == "sensor_1")

    //2.应用规则，把规则和数据流进行结合
    val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS, pattern1)

    //3.获取匹配的结果
    val resultDS: DataStream[String] = sensorPS.select(
      data => data.toString()
    )

    resultDS.print("cep")

    env.execute()
  }
}

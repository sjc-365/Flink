package com.atguigu.day07

import com.atguigu.day02.WaterSensor
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object CEP_Exec3 {

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
      /*//第一条数据满足条件
      .where(_.id == "sensor_1")
      //并且
      .next("next")
      //第二条数据满足条件
      .where(_.vc == 60)*/

      /*//满足条件后，开始匹配下一条数据，不会继续匹配下去
      .where(_.id == "sensor_1")
      .followedBy("follow")
      .where(_.id == "sensor_2")*/

      //遇到满足条件的数据后，还会继续匹配下去，继续生成新的数据结果
      .where(_.id == "sensor_1")
      .followedByAny("followAny")
      .where(_.id == "sensor_2")

    CEP.
      pattern(sensorDS,pattern2)
      .select(data => data.toString())
      .print("cep2")

    env.execute()

  }

}

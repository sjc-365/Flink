package com.atguigu.day08


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

//Table API 的创建、使用、输出
object SQL_TableAPI_Exec1 {
  def main(args: Array[String]): Unit = {
    //1.执行环境的搭建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.数据的读取
    val sourceDS: DataStream[WaterSensor] = env
      .readTextFile("input/sensor-data.log")
      .map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
      )
      .assignAscendingTimestamps(_.ts * 1000L)

    //3.创建表环境
    //3.1设置表属性
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance() //获取build对象
      .useOldPlanner() //使用老的执行计划
      .inStreamingMode() //基于流处理的模式
      .build()

    //3.2 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //4.创建表(把流转换为表，并给字段取名字)
    val table: Table = tableEnv.fromDataStream(sourceDS, 'id, 'ts, 'vc)

    //5.查询表
    val resultTable: Table = table
      .filter('id === "sensor_2")//判断相等用三个等于号
      .select('id, 'vc, 'ts)//select方法，选择要输出的字段

    //6.保存数据--连接外部系统
    //6.1 将表转换成流
    resultTable.toAppendStream[Row].print("table api")

    env.execute()
  }

  case class WaterSensor(id:String,ts:Long,vc:Double)
}

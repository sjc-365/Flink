package com.atguigu.day08

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row


//Table API读取外部系统的数据
object SQL_TableAPI_Exec5 {
  def main(args: Array[String]): Unit = {
    //1.执行环境的搭建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.创建表环境
    //3.1设置表属性
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance() //获取build对象
      .useOldPlanner() //使用老的执行计划
      .inStreamingMode() //基于流处理的模式
      .build()

    //3.2 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)


    //7.将结果保存到外部系统
    //7.1创建一个外部表，尚未插入数据
    tableEnv
      .connect(new FileSystem().path("output/aaa.txt"))
        .withFormat(new OldCsv())
        .withSchema(
          new Schema()
            .field("id",DataTypes.STRING())
            .field("timestamp",DataTypes.BIGINT())
            .field("vc",DataTypes.DOUBLE())
        )
        .createTemporaryTable("fsTable")

    //与创建好的表进行绑定，读取外部系统数据
    val table: Table = tableEnv.from("fsTable")

    //7.2 将查询的结果插入到创建的外部表中
    tableEnv
      .sqlQuery("select * from fsTable")
      .toAppendStream[Row]
        .print()

    env.execute()
  }

  case class WaterSensor(id:String,ts:Long,vc:Double)
}

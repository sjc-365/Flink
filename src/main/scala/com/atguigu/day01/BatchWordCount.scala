package com.atguigu.day01

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //1.获取环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.获取数据
    val fileDS: DataSet[String] = env.readTextFile("input/word.txt")

    //3.处理数据
    //拆分数据--展开数据--转换为k-v结果--分组--求和
   val rseult: AggregateDataSet[(String, Int)] = fileDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    //关闭资源、启动
    rseult.print()

  }

}

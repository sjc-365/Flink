package com.atguigu.day03

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object UV {

  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2.获取数据
    val fileDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")

    //转为样例类
    val UserDS: DataStream[UserBehavior] = fileDS.map(line => {
      val words: Array[String] = line.split(",")
      UserBehavior(
        words(0).toLong,
        words(1).toLong,
        words(2).toInt,
        words(3),
        words(4).toLong
      )
    })

    //过滤数据，将pv类型的数据筛选出来
    val filterDS: DataStream[UserBehavior] = UserDS.filter(data => data.behavior == "pv")


    //对user_id进行分组
    val useridDS: KeyedStream[(String, Long), String] = filterDS.map(data => ("uv", data.userId)).keyBy(_._1)

    //调用process函数，自定义处理逻辑
    val result: DataStream[Long] = useridDS.process(new MyKeyedProcessFunction)
    //取单个user_id
    result.print()

    env.execute()


  }

  class MyKeyedProcessFunction extends KeyedProcessFunction[String,(String, Long),Long] {
    //计数
    val uvCount: mutable.Set[Long] = mutable.Set[Long]()
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), Long]#Context, collector: _root_.org.apache.flink.util.Collector[Long]): Unit = {
     //将用户id存到set集合中。带有去重功能
      uvCount.add(i._2)
      //计数set集合的长度，及个数
      collector.collect(uvCount.size)
    }
}
  case class  UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
}

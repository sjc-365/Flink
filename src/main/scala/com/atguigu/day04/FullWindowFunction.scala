package com.atguigu.day04

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FullWindowFunction {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    // 3.分组、开窗
    // 正常来说，使用窗口，都是在分组之后
    val dataWS: WindowedStream[(String, Int), String, TimeWindow] = socketDS
      .map(data => (data, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5)) // 滚动窗口


    //全窗口函数，将数据全部收集起来后，等窗口要计算了再进行计算
    //ProcessWindowFunction[IN, OUT, KEY, W <: Window]参数类型
    dataWS.process(
      new ProcessWindowFunction[(String,Int),String,String,TimeWindow] {
        //对数据进行处理，主要是跟上下文有关的数据（窗口时间，窗口数据量等）进行交互
        //
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          out.collect(
            "当前key是=" + key
            + ",当前属于的窗口["+ context.window.getStart + "," + context.window.getEnd
              + "],一共有数据" + elements.size + "条数据"


          )
        }
      }
    ).print("process方法")


    env.execute()

  }


}

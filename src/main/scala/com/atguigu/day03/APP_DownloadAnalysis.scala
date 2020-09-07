package com.atguigu.day03

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object APP_DownloadAnalysis {
  def main(args: Array[String]): Unit = {
    //上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val fileDS: DataStream[MarketingUserBehavior] = env.addSource(new MySourec())

    //过滤数据，将下载的过滤出来
    val filterDS: DataStream[MarketingUserBehavior] = fileDS.filter(_.behavior == "DOWNLOAD")

    //根据统计维度进行分组
    filterDS
      .map(data => (data.channel,1))
      .keyBy(_._1)
      .sum(1)
      .print("filter")

    env.execute()

  }


  //自定义数据源
  class MySourec extends SourceFunction[MarketingUserBehavior] {
    var flag = true
    val userBehaviorList = List("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL")
    val channelList = List("HUAWEI", "XIAOMI", "OPPO", "VIVO")

    //数据的输出
    override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {

      //造数据
      while(flag){
        sourceContext.collect(
          MarketingUserBehavior(
            Random.nextInt(100).toLong,
            userBehaviorList(Random.nextInt(userBehaviorList.size)),
            channelList(Random.nextInt(channelList.size)),
            System.currentTimeMillis()
          )
        )
        Thread.sleep(1000L)
      }

    }

    //关闭
    override def cancel(): Unit = {
      flag = false

    }
}
       
  case class MarketingUserBehavior(
                                    userId: Long,
                                    behavior: String,
                                    channel: String,
                                    timestamp: Long)

}

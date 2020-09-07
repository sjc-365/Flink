package com.atguigu.day03


import org.apache.flink.streaming.api.scala._

object AdClickAnalysis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //将数据转换为样例类
    val adcDS = env
      .readTextFile("input/AdClickLog.csv")
      .map(line => {
        val words = line.split(",")
        AdClickLog(words(0).toLong, words(1).toLong, words(2), words(3), words(4).toLong)
      })
    //按照统计的维度进行分组（维度：省份、广告），多个维度可以拼接在一起
    val result = adcDS.map(log => (log.province + "_" +log.city + "_" +log.adId, 1)).keyBy(_._1).sum(1)

    result.print("adclick analysis by province and ad")

    env.execute()
  }
  case class AdClickLog(
                         userId: Long,
                         adId: Long,
                         province: String,
                         city: String,
                         timestamp: Long)
}

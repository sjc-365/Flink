package com.atguigu.day06

import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//需求：实时统计每小时内的网站UV
object ADClickCount{
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceDS: DataStream[AdClickLog] = env
      .readTextFile("input/ADClickLog.csv")
      .map(
        line => {
          val words: Array[String] = line.split(",")
          AdClickLog(
            words(0).toLong,
            words(1).toLong,
            words(2),
            words(3),
            words(4).toLong)
        }
      )
      //设置时间抽取和WaterMark的生成
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //开窗统计浏览量
    val resultDS: DataStream[String] = sourceDS
      .keyBy(data => (data.province, data.adId))
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(
        //预聚合函数，对分组数据进行汇总处理
        new MyAggfunction,
        //全窗口函数，对汇总结果打上标签，方便后续进行再分组
        new MyWinFunction
      )
      .keyBy(_.windowEnd)
      //分组后对数据进行处理，包括排序、取topN
      .process(
        new MyProFunction
      )

    resultDS.print("广告点击top3")

    env.execute()
  }

  class MyProFunction extends KeyedProcessFunction[Long,HotAdClick,String] {
    //设置状态，和定时器，保证定时器触发时，停止收集同组数据，并进行排序
    var timeTs: ValueState[Long] = _
    var hotAdList: ListState[HotAdClick] = _

    //获取状态
    override def open(parameters: Configuration): Unit = {
      timeTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeTs",classOf[Long]))
      hotAdList = getRuntimeContext.getListState(new ListStateDescriptor[HotAdClick]("hotAdList",classOf[HotAdClick]))
    }
    //收集数据
    override def processElement(value:HotAdClick, ctx: KeyedProcessFunction[Long, HotAdClick, String]#Context, out: Collector[String]): Unit = {
      //该组数据第一次来，则进行注册
      if(timeTs.value() == 0){
        //注册定时器，触发事件是当数据的wm达到设置的时间时，触发
        ctx.timerService().registerEventTimeTimer(value.windowEnd)
        //
        timeTs.update(value.windowEnd)
      }
      hotAdList.add(value)
    }

    //定时器触发，对数据进行排序，输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotAdClick, String]#OnTimerContext, out: Collector[String]): Unit = {
      //转化数据类型，取出数据
      val hotIt: util.Iterator[HotAdClick] = hotAdList.get().iterator()
      import scala.collection.JavaConverters._
      val hotADList: List[HotAdClick] = hotIt.asScala.toList
      //排序，取TopN
      val top3: List[HotAdClick] = hotADList.sortWith(_.clickCount > _.clickCount).take(3)

      //输出
      out.collect(
        s"""
          |窗口结束时间:${new Timestamp(timestamp)}
          |--------------------------------------
          |${top3.mkString("\n")}
          |--------------------------------------
          |""".stripMargin
      )

    }
  }

  //全窗口函数，给汇总数据打上时间标签
  class MyWinFunction extends ProcessWindowFunction[Long,HotAdClick,(String, Long),TimeWindow] {
    override def process(key: (String, Long), context: Context, elements: Iterable[Long], out: Collector[HotAdClick]): Unit = {
      out.collect(HotAdClick(key._1,key._2,elements.iterator.next(),context.window.getEnd))

    }
  }

  //预聚合函数
  class MyAggfunction extends AggregateFunction[AdClickLog,Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(in: AdClickLog, acc: Long): Long = acc + 1L
    override def getResult(acc: Long): Long = acc
    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }
  case class HotAdClick(province: String, adId: Long, clickCount: Long, windowEnd: Long)

  case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
}

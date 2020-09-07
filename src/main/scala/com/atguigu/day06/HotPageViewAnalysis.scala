package com.atguigu.day06

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
* 需求：每隔5秒，输出最近10分钟内访问量最多的前N个URL
* 处理逻辑：滑步窗口、以URL为分组依据、预聚合后全窗口中给数据添加窗口结束时间标签
*         以窗口结束时间为分组，再结合状态，将数据保存下来，等待定时器触发后，统一对数据进行排序和取topN
* */
object HotPageViewAnalysis {

  def main(args: Array[String]): Unit = {
    //搭建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //获取数据.转为样例类，进行时间抽取和wm生成
    val sourceDS: DataStream[ApacheLog] = env
      .readTextFile("input/apache.log")
      .map(
        line => {
          val words: Array[String] = line.split(" ")
          val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          val time: Long = format.parse(words(3)).getTime
          ApacheLog(
            words(0),
            words(1),
            time,
            words(5),
            words(6)
          )
        }
      )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
          override def extractTimestamp(element: ApacheLog): Long = element.eventTime * 1000L
        }
      )

    //对数据进行处理
    //分组、使用聚合函数、全窗口函数
    //
    val ResultDS: DataStream[String] = sourceDS
      .keyBy(a => a.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(
        new Myaggregate,
        new MyWindowFunction
      )
      .keyBy(a => a.windowEnd)
      .process(
        new MyKeyedProcess
      )

    ResultDS.print("热门页面")

    env.execute()

  }

  //对数据进行收集、排序、取topN
  class MyKeyedProcess extends KeyedProcessFunction[Long,HotPageClick,String] {
    //对数据进行收集，需要定时器和状态的参与 == 需要状态变量、ontime函数、open函数
    var timeTS: ValueState[Long] = _
    var dataList: ListState[HotPageClick] = _

    override def open(parameters: Configuration): Unit = {
      timeTS = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeTS",classOf[Long]))
      dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotPageClick]("dataList",classOf[HotPageClick]))
    }

    override def processElement(value: HotPageClick, ctx: KeyedProcessFunction[Long, HotPageClick,String]#Context, out: Collector[String]): Unit = {
      //如果是第一条数据来，则注册定时器
      if(timeTS.value() == 0){
        //注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd)
      }
      dataList.add(value)
    }

    //触发定时器，执行相应的操作
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotPageClick, String]#OnTimerContext, out:Collector[String]): Unit = {
      //转化状态类型，使其能够调用sort、take方法

      //获取状态中保存的数据
      val dataIt: util.Iterator[HotPageClick] = dataList.get().iterator()

      //将数据转化为scala集合，方便进行排序和取topN
      import scala.collection.JavaConverters._
      val listBuffer: Iterator[HotPageClick] = dataIt.asScala

      val top3: List[HotPageClick] = listBuffer.toList.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

      //输出top3
      out.collect(
        s"""
          |窗口结束时间:${new Timestamp(timestamp)}
          |--------------------------------------
          |${top3.mkString("\n")}
          |""".stripMargin
        )

      //清空状态
      dataList.clear()
      timeTS.clear()

    }
  }

  //全窗口函数
  class MyWindowFunction extends ProcessWindowFunction[Long,HotPageClick,String,TimeWindow] {
    //给数据添加上窗口结束的时间标签
    override def process(key: String, context: Context, elements: Iterable[Long],out:Collector[HotPageClick]): Unit = {
      out.collect(HotPageClick(key,elements.iterator.next(),context.window.getEnd))

    }
  }

  //自定义预聚合函数，实现数据来一条处理一条，减少内存的消耗
  class Myaggregate extends AggregateFunction[ApacheLog,Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(in: ApacheLog, acc: Long): Long = acc+1L
    override def getResult(acc: Long): Long = acc
    override def merge(acc: Long, acc1: Long): Long = acc+acc1

  }

  //商品点击次数统计样例类
  case class HotPageClick(url: String, clickCount: Long, windowEnd: Long)

  case class ApacheLog(ip:String, userId:String, eventTime:Long, method:String, url:String)
}

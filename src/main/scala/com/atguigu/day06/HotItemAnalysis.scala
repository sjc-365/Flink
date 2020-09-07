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


/**需求：每隔5分钟输出最近一小时内点击量最多的前N个商品
 * 数据来源：”input/UserBehavior.csv“
 * 数据处理：
 *    1.设置时间语义，设置时间抽取和WaterMark的生成
 *    2.分组，开窗
 *    3.process方法下调用agg预聚合函数、全窗口函数
 *        1）分组之后数据进入预聚合函数，每来一条数据就处理一次，保证数据不挤压，同时最后将结构输出给全窗口函数
 *        2）全窗口函数给结果打上标签，方便取出
 *
 */
object HotItemAnalysis {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceDS: DataStream[UserBehavior] = env
      .readTextFile("input/UserBehavior.csv")
      .map(
        line => {
          val words: Array[String] = line.split(",")
          UserBehavior(
            words(0).toLong,
            words(1).toLong,
            words(2).toInt,
            words(3),
            words(4).toLong)
        }
      )
      //设置时间抽取和WaterMark的生成
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //获取热门商品统计情况
    val aggDS: DataStream[HotItemClick] = sourceDS
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //窗口函数，里面可以传两个函数（一个预聚合函数，一个全窗口函数）
      .aggregate(
        //预聚合函数，每来一条数据就累加一次，直到窗口触发的时候才会把 统计的结果值 传递给 全窗口函数
        new MyAggregateFunction,
        new MyWindowFunction
      )

    //从全窗口的热门商品统计流中对结束时间进行分组，并对数据进行排序，取topN
    val resultDS: DataStream[String] = aggDS
      .keyBy(_.windowEnd)
      .process(
        new MyKeyedProcessFunction
      )

    //输出结果
    resultDS.print("热门商品前三")

    env.execute()

  }

  //自定义分组处理函数,分组后的数据进行排序和取topN
  class MyKeyedProcessFunction extends KeyedProcessFunction[Long,HotItemClick,String] {
    //需要等数据全部到齐了，再进行排序，因此需要设置一个定时器，当达到关窗数据，触发定时器
    //数据也停止收集，开始对数据进行排序

    //一个关于时间的值状态
    var timeTs: ValueState[Long] = _
    //一个保存数据的list状态
    var dataList: ListState[HotItemClick] = _

    //获取状态值
    override def open(parameters:Configuration): Unit = {
      timeTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeTs",classOf[Long]))
      dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotItemClick]("dataList",classOf[HotItemClick]))

    }

    //注册定时器
    override def processElement(value:HotItemClick, ctx: KeyedProcessFunction[Long, HotItemClick, String]#Context, out:Collector[String]): Unit = {
      //1.如果是第一条数据来，注册定时器
      if(timeTs.value() == 0){
        ctx.timerService().registerEventTimeTimer(value.windowEnd)
        timeTs.update(value.windowEnd)
      }
      //2.保存数据
      dataList.add(value)

    }

    //触发定时器，对数据进行排序取topN
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,HotItemClick,String]#OnTimerContext, out:Collector[String]): Unit = {
      //获取状态中保存的数据
      val dataIt: util.Iterator[HotItemClick] = dataList.get().iterator()

      //将数据转化为scala集合，方便进行排序和取topN
      import scala.collection.JavaConverters._
      val listBuffer: Iterator[HotItemClick] = dataIt.asScala

      //开始排序
      val clickCountTop3: List[HotItemClick] = listBuffer.toList.sortWith(_.clickCount > _.clickCount).take(3)

      //清空两个状态
      dataList.clear()
      timeTs.clear()

      //输出
      out.collect(
        s"""
          |窗口结束时间：${new Timestamp(timestamp)}
          |---------------------------------------
          |Top3商品：${clickCountTop3.mkString("\n")}
          |""".stripMargin
      )
    }


  }

  //自定义预聚合函数,传入UserBehavior，累计器类型为Long,输出类型为Long
  class MyAggregateFunction extends AggregateFunction[UserBehavior,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1L

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1

  }

  //自定义全窗口函数，输入是预聚合函数的输出，将所有key组下的数据输出到同一个DS中
  class MyWindowFunction extends ProcessWindowFunction[Long,HotItemClick,Long,TimeWindow] {

    override def process(key: Long, context: Context, elements: Iterable[Long], out:Collector[HotItemClick]): Unit = {
      //将收到的数据打上窗口关闭的时间标签,并封装成HotItemClick样例类，输出出去
      out.collect(HotItemClick(key,elements.iterator.next(),context.window.getEnd))
    }
  }

  //商品点击次数统计样例类
  case class HotItemClick(itemId: Long, clickCount: Long, windowEnd: Long)

  //商品样例类
  case class  UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}

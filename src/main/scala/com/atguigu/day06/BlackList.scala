package com.atguigu.day06

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**过滤黑名单 ：一天内同一用户同一个广告点击超过100次的
 * //同时对正常数据进行处理
 * 把黑名单过滤处理，放在侧输出流，进行告警--keyby之后进行开窗，调
 * 用keyedProcess方法，对分组数据进行求和，当达到100次则放入侧输出流，没有则放到下游中
 * 仅只告警一次（判断），其他数据正常传递至下游
 *
 *
 */

object BlackList {

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

    //先不开窗，过滤出黑名单（统计的时间为维度是一整天，下面开窗的维度是一个小时，故不开窗）
    val filterDS: DataStream[AdClickLog] = sourceDS
      .keyBy(data => (data.userId, data.adId))
      .process(new MykeyProcess)

    //将告警信息打印出来
    val alarmTag: OutputTag[String] = new OutputTag[String]("alarm")
    filterDS.getSideOutput(alarmTag).print("blackList")

    //正常的取topN
    filterDS
      .keyBy(data => (data.userId,data.adId))
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(
        //取topN先agg函数做预聚合，再wind函数打上标签，再通过windend进行分组，最后通过keyopro函数进行排序
        new MyFilterAgg,
        new MyFilterWin
      )
      .keyBy(_.windowEnd)
      .process(
        new MytopPro
      ).print("nomal")


    env.execute()
  }

  //对结果进行输出
  class MytopPro extends KeyedProcessFunction[Long,UserHotAdClick,String] {
    override def processElement(value: UserHotAdClick, ctx: KeyedProcessFunction[Long, UserHotAdClick, String]#Context, out: Collector[String]): Unit = {
      out.collect("在时间"+new Timestamp(value.windowEnd)+"，5分钟内，用户"+value.userId+"，对广告："+value.adId+"，点击了"+value.clickCount+"次")

    }
}

  //对汇总数据打上窗口结束的标签
  class MyFilterWin extends ProcessWindowFunction[Long,UserHotAdClick,(Long,Long),TimeWindow] {

    override def process(key: (Long, Long), context: Context, elements: Iterable[Long],out:Collector[UserHotAdClick]): Unit = {
      out.collect(UserHotAdClick(key._1,key._2,elements.iterator.next(),context.window.getEnd))
    }
  }

  //预聚合，对每组数据进行累加
  class MyFilterAgg extends AggregateFunction[AdClickLog,Long,Long] {

    override def createAccumulator(): Long = 0L

    override def add(in: AdClickLog, acc: Long): Long = acc +1L

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  //过滤黑名单
  class MykeyProcess extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog] {
    //定义状态保存，每个用户对某个广告的点击次数
    var clickCount: ValueState[Int] = _
    var timeTs:Long = 0L
    //设置一个布尔状态，当警告过一次后，则变化状态，之后不再警告
    var alarmFlag: ValueState[Boolean] = _

    override def open(parameters:Configuration): Unit = {
      clickCount = getRuntimeContext.getState(new ValueStateDescriptor[Int]("clickCount",classOf[Int]))
      alarmFlag = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("alarmFlag",classOf[Boolean]))

    }

    //注册定时器，将数据收集起来
    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog,AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
      //取出值
      var currentCount:Int = clickCount.value()
      //第一次进来
      if(currentCount == 0){
        //将时间转化为天数
        val currentDay: Long = value.timestamp * 1000L / (24 * 60 * 60 * 1000L)
        //增加天数
        val nextDay:Long = currentDay +1
        //将24点转为为时间戳
        val nextDayStartTs: Long = nextDay* (24 * 60 * 60 * 1000L)

        //注册24点的定时器，同时同一组只注册一次
        if(timeTs == 0){
          ctx.timerService().registerEventTimeTimer(nextDayStartTs)
          timeTs = nextDayStartTs
        }

      }

      //侧输出流
      if(currentCount >= 100){
        //未发生改变，状态依旧是false
        if(!alarmFlag.value()){
          //告警信息输出到侧输出流
          val alarmTag: OutputTag[String] = new OutputTag[String]("alarm")
          ctx.output(alarmTag,"用户"+value.userId+"对广告"+value.adId+"今日点击已超过100次，存在恶意点击的可能")
          alarmFlag.update(true)
        }
        //没有达到100则进行累加
      }else{
        clickCount.update(currentCount+1)
        //用户之前的99次点击一样会传递给下游
        out.collect(value)
      }

    }

    //
  }

  case class UserHotAdClick(userId: Long, adId: Long, clickCount: Long, windowEnd: Long)

  case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

}

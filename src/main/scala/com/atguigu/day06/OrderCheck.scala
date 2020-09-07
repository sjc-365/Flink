package com.atguigu.day06

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


//需求：当支付订单和下单订单超过15分钟时，则认为存在异常
object OrderCheck {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceDS: DataStream[OrderEvent] = env
      .readTextFile("input/OrderLog.csv")
      .map(
        line => {
          val words: Array[String] = line.split(",")
          OrderEvent(
            words(0).toLong,
            words(1),
            words(2),
            words(3).toLong)
        }
      )
      //设置时间抽取和WaterMark的生成
      .assignAscendingTimestamps(_.eventTime * 1000L)


    //对数据进行处理
    //1.对数据orderId进行分组
    val resultDS: DataStream[String] = sourceDS
      .keyBy(_.orderId)
      //调用keyedProcessFunction方法
      //1.针对只来一条的数据，用状态和定时器进行触发处理
      //2.针对两条都来的数据，利用状态和时间间隔来判断，是否超时，超时的输入到侧输出流，未超时的正常输出到下游
      .process(
        new KeyedProcessFunction[Long, OrderEvent, String] {
          //保存下单数据来的时间
          var orderTS: ValueState[OrderEvent] = _
          //保存支付数据来的时间
          var payTS: ValueState[OrderEvent] = _
          //保存时间状态
          var timeoutTs: ValueState[Long] = _
          //创建侧输出流
          val timeoutTag = new OutputTag[String]("timeout")

          //获取状态
          override def open(parameters: Configuration): Unit = {
            orderTS = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("orderTS", classOf[OrderEvent]))
            payTS = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payTS", classOf[OrderEvent]))
            timeoutTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeoutTs", classOf[Long]))

          }

          //无论谁先来，注册一个定时器，15分钟没来，触发定时器，来了则删除定时器
          override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]): Unit = {

            if (timeoutTs.value() == 0) {
              //更新数据
              timeoutTs.update(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000L)
              //注册定时器，设定的触发事件是当前机器时间的15分钟后
              //在实时数据流下，数据本身带有的事件事件无法作为触发条件，因为下单数据和支付数据的生成可能3秒
              //如果pay先来，等了15分钟，order都没来，就是异常
              //如果order先来，等了15分钟，pay还没来，也是异常
              ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000L)
            } else {
              //第二条来了，删定时器
              ctx.timerService().deleteProcessingTimeTimer(timeoutTs.value())
              timeoutTs.clear()

            }

            //接下来的逻辑来判断都来了，时长是否超过15分钟
            //如果来的数据是order
            if (value.eventType == "create") {
              //检查pay来了没有
              if (payTS.value() == null) {
                //没来，则把order存起来
                orderTS.update(value)
                //如果pay订单来了
              } else {
                //比较两者数据是否相差15分钟
                if (payTS.value().eventTime - value.eventTime > 15 * 60) {
                  //将结果输出到侧输出流
                  ctx.output(timeoutTag, "订单" + value.orderId + "支付成功，但超时，请检查是否存在异常")
                } else {
                  //未超时，正常输出
                  out.collect("订单" + value.orderId + "支付成功")
                }
                payTS.clear()
              }

              //来的数据是pay
            } else {
              //检查order来了没有
              if (orderTS.value() == null) {
                //没来，则把order存起来
                payTS.update(value)
                //如果order订单来了
              } else {
                //比较两者数据是否相差15分钟
                if (value.eventTime - orderTS.value().eventTime > 15 * 60) {
                  //将结果输出到侧输出流
                  ctx.output(timeoutTag, "订单" + value.orderId + "支付成功，但超时，请检查是否存在异常")
                } else {
                  //未超时，正常输出
                  out.collect("订单" + value.orderId + "支付成功")
                }
                orderTS.clear()
              }

            }

          }

          //触发定时器，输出异常
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
            //判断谁没来。以便异常信息更加精准
            if (orderTS.value() != null) {
              ctx.output(timeoutTag, "下单数据" + orderTS.value().orderId + "来了，但支付数据15分钟依旧没有来")
              orderTS.clear()
            }

            if (payTS.value() != null) {
              ctx.output(timeoutTag, "支付数据" + payTS.value().orderId + "来了，但下单数据15分钟依旧没有来")
              payTS.clear()
            }

            //清空timeoutTs
            timeoutTs.clear()

          }
        }
      )

    //输出结果
    resultDS.print("nomal")
    //输出异常结果
    val timeoutTag = new OutputTag[String]("timeout")
    resultDS.getSideOutput(timeoutTag).print("timeout")

    env.execute()

  }

  case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
}

package com.atguigu.day06

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


//需求：同一用户在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
object LoginDetect {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceDS: DataStream[LoginEvent] = env
      .readTextFile("input/LoginLog.csv")
      .map(
        line => {
          val words: Array[String] = line.split(",")
          LoginEvent(
            words(0).toLong,
            words(1),
            words(2),
            words(3).toLong)
        }
      )
      //设置时间抽取和WaterMark的生成
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
          override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
        }
      )

    //对数据进行分析：
    //  1.连续两次（及以上）失败，且时长不超过2秒，则判断恶意登录（如何筛选出来CEP）
    //  2.两次登录失败（中间内有成功），时长在2秒内，是否属于恶意登录。不属于可能是登录之后退出再登录


    // 当前进行的操作是将失败数据过滤出来，然后进行两两之间的判断，如果时间间隔小于2秒,则判定是恶意登录
    sourceDS.
      //过滤
      filter(_.eventType == "fail")
      //分组
      .keyBy(_.userId)
      //统计每组结果
      .process(
        new KeyedProcessFunction[Long,LoginEvent,String] {
          //设置一个状态，用来保存结果
          var loginCount: ValueState[LoginEvent] = _

          //获取状态
          override def open(parameters: Configuration): Unit = {
            loginCount = getRuntimeContext.getState(new ValueStateDescriptor[LoginEvent]("loginCount",classOf[LoginEvent]))
          }

          override def processElement(value: LoginEvent, ctx:KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]): Unit = {
            if(value.eventType == "fail"){}
              //第一次登录失败
              if(loginCount.value() == null){
                loginCount.update(value)
              }else{
                //第二次及以上登录失败
                //判断时间是否相差小于2秒，如果是则进行输出
                if(Math.abs(value.eventTime - loginCount.value().eventTime) <= 2){
                  out.collect("用户"+value.userId+"2秒内连续2次登录失败，可能为恶意登录")
                }
              }

          }
        }
      )
      .print("result")

    env.execute()
  }

  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

}

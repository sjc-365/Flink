package com.atguigu.day05

import com.atguigu.day02.WaterSensor
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object State_KeyedDemo {

  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据.乱序数据，设置了1秒的等待时间
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost", 9999)
      .map(data => {
        val words: Array[String] = data.split(",")
        WaterSensor(words(0), words(1).toLong, words(2).toDouble)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(1)) {
          override def extractTimestamp(element: WaterSensor): Long = element.ts * 1000L
        }
      )


    //转换数据类型 设置WarteMark为间隔性生成wm
    val resultDS: DataStream[String] = socketDS
      .keyBy(_.id)
      .process(
        new KeyedProcessFunction[String, WaterSensor, String] {
          //TODO 获取值类型的状态
          //初始化状态
          var state: ValueState[Long] = _
          var listState: ListState[Long] = _
          var mapState: MapState[String,Long] = _

          /*1.通过懒加载获取状态
          private lazy val state: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("value state",classOf[Long]))*/

          //2.在open方法中获取状态
          override def open(parameters: Configuration): Unit = {
            //获取值类型状态
            state = getRuntimeContext.getState(new ValueStateDescriptor[Long]("value state", classOf[Long]))
            listState = getRuntimeContext.getListState(new ListStateDescriptor[Long]("list state",classOf[Long]))
            mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("map state",classOf[String],classOf[Long]))

          }

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
            /*// 值类型的操作
            state.update(1L)  //值类型的更新操作
            state.value() // 值类型的状态获取值
            // state.clear() // 清空状态
            // 列表类型
            listState.add(2L) // 添加单个值
            listState.addAll(java.util.List[Long](3L,4L))  // 添加一个List
            listState.update()  // 更新整个List
            //liststate.clear() // 清空状态

            // Map类型  - 操作基本同 HashMap
            mapState.put()  // 添加或更新数据
            mapState.putAll() // 整个Map
            mapState.remove() // 删除某个值
            mapState.contains() // 是否包含某个key
            //mapState.get()  // 获取指定key的值*/

          }
        }
      )

    resultDS.print("state Demo")

    env.execute()
  }
}

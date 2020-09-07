package com.atguigu.day02

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

class TransfromTest {

  //正常调用算子（匿名函数）
  @Test
  def MapTest1: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val listDS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6))

    listDS.map((_,1)).print()

    env.execute()
  }

  //调用自定义函数类
  @Test
  def MapTest2: Unit ={

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val mapDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    //传入一个自定义函数
    mapDS.map(new MyMapFunction()).print()

    env.execute()
  }


  //调用rich函数
  @Test
  def MapTest3: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.readTextFile("input/sensor-data.log").map(new MyRichMap).print()

    env.execute()

  }


  @Test
  def flatMapp: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val list = List(List(1, 2, 3, 4), List(5, 6, 7))
    env.fromCollection(list).flatMap(list => list).print()
    //env.fromCollection(list).flatMap(x => x :+ 1).print()

    env.execute()

  }

  @Test
  def filter: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //在filter函数中直接输入布尔表达式，元素依次进行判断，符合条件的才会留下来
    env.fromCollection(List(1,2,3,4,5)).filter(_>3).print()

    env.execute()

  }

  @Test
  def KeyByTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val value: DataStream[String] = env.readTextFile("input/sensor-data.log")

    value.map(line => {
      val strings: Array[String] = line.split(",")

      (strings(0),(WaterSensor(strings(0),strings(1).toLong,strings(1).toDouble)))

    }).keyBy(_._1).print()

    env.execute()
  }

  @Test
  def SufferTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val value: DataStream[String] = env.readTextFile("input/sensor-data.log")

    //处理数据
    val shuffle: DataStream[String] = value.flatMap(_.split(",")).shuffle

    shuffle.print("shuffle")

    env.execute()
  }

  @Test
  def SpiltTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val value: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val result: SplitStream[WaterSensor] = value.map(line => {
      val words: Array[String] = line.split(",")
      WaterSensor(words(0), words(1).toLong, words(2).toDouble)
    }).split(semson => {
      if (semson.vc >= 40) {
        Seq("alarm")
      } else if (semson.vc >= 30) {
        Seq("warn")
      } else {
        Seq("normal")
      }
    })

    env.execute()

  }

  @Test
  def SelectTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val value: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val result: SplitStream[WaterSensor] = value.map(line => {
      val words: Array[String] = line.split(",")
      WaterSensor(words(0), words(1).toLong, words(2).toDouble)
    }).split(semson => {
      if (semson.vc >= 40) {
        Seq("alarm")
      } else if (semson.vc >= 30) {
        Seq("warn")
      } else {
        Seq("normal")
      }
    })

    val value1: DataStream[WaterSensor] = result.select("alarm")
    val value2: DataStream[WaterSensor] = result.select("warn")
    val value3: DataStream[WaterSensor] = result.select("normal")

    value1.print("alarm")
    value2.print("warn")
    value3.print("normal")

    env.execute()

  }

  @Test
  def ConnectTest: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
    env.setParallelism(1)

    val watDS: DataStream[WaterSensor] = sensorDS.map(line => {
      val words: Array[String] = line.split(",")
      WaterSensor(words(0), words(1).toLong, words(2).toDouble)
    })

    val spiltDS: SplitStream[WaterSensor] = watDS.split(sensor => {
      if (sensor.vc >= 40) {
        Seq("alarm")
      } else if (sensor.vc >= 30) {
        Seq("warn")
      } else {
        Seq("normal")
      }
    })

    val result1: DataStream[WaterSensor] = spiltDS.select("normal")
    val result2: DataStream[Int] = env.fromCollection(List(1, 2))

    val finalresult: ConnectedStreams[WaterSensor, Int] = result1.connect(result2)

    finalresult.
      map(
        sensor => sensor.id,
        num => num+1
      )
      .print()

    env.execute()

  }


  @Test
  def UoionTest: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
    env.setParallelism(1)

    val watDS: DataStream[WaterSensor] = sensorDS.map(line => {
      val words: Array[String] = line.split(",")
      WaterSensor(words(0), words(1).toLong, words(2).toDouble)
    })

    val spiltDS: SplitStream[WaterSensor] = watDS.split(sensor => {
      if (sensor.vc >= 40) {
        Seq("alarm")
      } else if (sensor.vc >= 30) {
        Seq("warn")
      } else {
        Seq("normal")
      }
    })

    val result1: DataStream[WaterSensor] = spiltDS.select("normal")
    val result2: DataStream[WaterSensor] = spiltDS.select("warn")

   result1.union(result2).print()

    env.execute()

  }

  @Test
  def RollTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")


    val dataDS: DataStream[(String, Double, Int)] = sensorDS
      .map(
        line => {
          val words: Array[String] = line.split(",")
          WaterSensor(words(0), words(1).toLong, words(2).toDouble)
        })
      .map(sensor => (sensor.id, sensor.vc, 1))

    val result: DataStream[(String, Double, Int)] = dataDS.keyBy(_._1).max(1)

    result.print()

    env.execute()
  }

  @Test
  def ProcessTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val dataDS: DataStream[(String, Double)] = sensorDS
      .map(
        line => {
          val words: Array[String] = line.split(",")
          WaterSensor(words(0), words(1).toLong, words(2).toDouble)
        })
      .map(sensor => (sensor.id, sensor.vc))

    dataDS.keyBy(_._1).process(new MykeyedProcessFunction).print()

    env.execute()

  }

  @Test
  def ReduceTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val dataDS: DataStream[(String, Double)] = sensorDS
      .map(
        line => {
          val words: Array[String] = line.split(",")
          WaterSensor(words(0), words(1).toLong, words(2).toDouble)
        })
      .map(sensor => (sensor.id, sensor.vc))

    dataDS.keyBy(_._1).reduce((s1,s2) =>{
      (s1._1,s1._2+s2._2)
    }).print()

    env.execute()

  }







}

class MykeyedProcessFunction extends KeyedProcessFunction[String,(String,Double),String ] {
  override def processElement(i: (String, Double), context: KeyedProcessFunction[String, (String, Double), String]#Context, collector: Collector[String]): Unit = {
    collector.collect("我来到process啦，分组的key是="+context.getCurrentKey+",数据="+i)
  }
}

class MyMapFunction extends MapFunction[String,WaterSensor]{

  override def map(t: String): WaterSensor = {
    //切割数据
    val strings: Array[String] = t.split(",")
    //将数据封装成样例类
    WaterSensor(strings(0),strings(1).toLong,strings(2).toDouble)
  }
}

//自定义map算子rich函数
class MyRichMap extends RichMapFunction[String,(Int,WaterSensor)]{
  override def map(in: String): (Int, WaterSensor) = {
    val words: Array[String] = in.split(",")
    //返回值
    (this.getRuntimeContext.getIndexOfThisSubtask,WaterSensor(words(0),words(1).toLong,words(2).toDouble))

  }

  override def open(parameters: Configuration): Unit =
    super.open(parameters)

  override def close(): Unit = super.close()
}
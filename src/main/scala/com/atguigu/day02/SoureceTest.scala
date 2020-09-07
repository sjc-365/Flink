package com.atguigu.day02

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.junit.Test

class SoureceTest {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  //从集合读取数据
  @Test
  def collectTest: Unit ={

    val list = List(1, 2, 3, 4, 5)
    env.fromCollection(list).print()
    env.execute()
  }

  //从本地文件读取数据
  @Test
  def localTest: Unit ={
    env.readTextFile("input/word.txt").flatMap(_.split(" ")).print()
    env.execute()
  }

  //从hdfs读取数据
  @Test
  def hdfsTest: Unit ={
    env.readTextFile("hdfs://hadoop102:9820/input/word.txt").flatMap(_.split(" ")).print()
    env.execute()
  }



  //从kafka读取数据
  @Test
  def kafkaTest: Unit ={

    //配置kafka消费者参数
    val properties = new Properties()
    properties.put("bootstrap.servers","hadoop102:9092")
    properties.put("group.id","consumer-flink")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset","latest")

    val KafkaDS: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String](
        "flink",
        new SimpleStringSchema(),
        properties
      )
    )

    KafkaDS.print()
    env.execute()

  }

  //自定义数据源
  @Test
  def MySourceTest: Unit ={
    val mysource = new Mysource
    env.addSource(mysource).print()

    /*
    Thread.sleep(10000)

     mysource.cancel()*/

    env.execute()
  }


}
//创建自定义数据源
class Mysource extends SourceFunction[WaterSensor]{
  var flag = true
  override def run(sourceContext: SourceFunction.SourceContext[WaterSensor]): Unit = {
    while (flag){
      //采集数据
      sourceContext.collect(
        //现场造数据
        WaterSensor(
          ""+new Random().nextInt(10),1598612809,new Random().nextInt(50)
        )
      )
      //休眠2秒
      Thread.sleep(2000)
    }

  }

  override def cancel(): Unit = {
    flag=false

  }
}

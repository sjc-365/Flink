package com.atguigu.day02

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.junit.Test
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


class SinkTest{

  @Test
  def KafkaSinkTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val fileDS: DataStream[String] = env.readTextFile("input/word.txt")

    fileDS.addSink(
      new FlinkKafkaProducer011[String](
        "hadoop102:9092","sensor",new SimpleStringSchema()
      )
    )
    env.execute()

  }

  @Test
  def RedisSinkTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val fileDS: DataStream[String] = env.readTextFile("input/word1.txt")

    val conf= new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

    fileDS.addSink(
      new RedisSink[String](
        conf,new RedisMapper[String] {
          //这里的key是redis最外层的key
          override def getCommandDescription: RedisCommandDescription = {
            new RedisCommandDescription(RedisCommand.HSET,"seneor0317")
          }

          //数据类型，hash类型的kay
          override def getKeyFromData(t: String): String = {
            t.split(" ")(0)
          }

          //hash类型的value
          override def getValueFromData(t: String): String = {
            t.split(" ")(1)

          }
        }
      )
    )

    env.execute()

  }

  @Test
  def ESTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val fileDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val mapDS: DataStream[WaterSensor] = fileDS.map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    //保存到ES中
    //val httphosts = new util.ArrayList[HttpHost]()
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102",9200))
    httpHosts.add(new HttpHost("hadoop103",9200))
    httpHosts.add(new HttpHost("hadoop104",9200))

    val esSink: ElasticsearchSink[WaterSensor] = new ElasticsearchSink.Builder[WaterSensor](
      httpHosts, new ElasticsearchSinkFunction[WaterSensor] {
        override def process(t: WaterSensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("start...")
          val dataMap = new util.HashMap[String, String]()
          dataMap.put("data", t.toString)
          val request: IndexRequest = Requests.indexRequest("sensor0317").`type`("reading").source(dataMap)
          requestIndexer.add(request)
          println("stop...")
        }
      }
    ).build()

    mapDS.addSink(esSink)

    env.execute()

  }

  @Test
  def MysqlTest: Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val fileDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val mapDS: DataStream[WaterSensor] = fileDS.map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    //自定义sinkfunction
    mapDS.addSink(new MySinkFunction())

    // 5. 执行
    env.execute()

  }



}

class MySinkFunction extends RichSinkFunction[WaterSensor]{

  var conn:Connection =_
  var patmt: PreparedStatement = _

  //做一个初始化操作.预先建好库和表
  override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/mydb","root","123456")
    patmt = conn.prepareStatement("INSERT INTO sensor VALUES(?,?,?)")
  }

  override def invoke(value: WaterSensor, context: SinkFunction.Context[_]): Unit = {
    patmt.setString(1,value.id)
    patmt.setLong(2,value.ts)
    patmt.setDouble(3,value.vc)
    patmt.execute()
  }

  override def close(): Unit = {
    patmt.close()
    conn.close()
  }

}
package com.zut.edu.SparkStreaming.Kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}



object SparkStreamingFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkStreamingKafka")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))

    val fromTopic = "t1"
    val brokers = "hadoop0:9092,hadoop1:9092,hadoop2:9092"

    //创建kafka连接参数
    val kafkaParams = Map[String,Object](
      //集群地址
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->brokers,
      //Key与Value的反序列化类型
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      //创建消费者组
      ConsumerConfig.GROUP_ID_CONFIG->"StreamingKafka",
      //自动移动到最新的偏移量
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG->"latest",
      //启用自动提交，将会由Kafka来维护offset【默认为true】
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG->"true"
    )

    //DStream
    val dStream = KafkaUtils.createDirectStream(
      ssc,  //SparkStreaming操作对象
      LocationStrategies.PreferConsistent,  //数据读取后如何分布在各个分区上
      /**
       * PreferBrokers：仅在 spark 的 executor 在相同的节点上，优先分配到存在kafka broker的机器是
       * PreferConsistent：大多数情况下使用，一致性的方式分配分区所有的 executor 上。（为了均匀分布）
       *PreferFixed：如果负载不均，可用通过这种方式来手动指定分配方式，其他没用在 map 中指定的，均采用 preferConsistent() 的方式分配
       *
       * */

      ConsumerStrategies.Subscribe[String,String](Array(fromTopic),kafkaParams)
    )

    val result = dStream.map(x => x.value()+"---------")
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

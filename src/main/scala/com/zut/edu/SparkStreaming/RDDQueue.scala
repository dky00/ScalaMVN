package com.zut.edu.SparkStreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueue {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("queue")
      .getOrCreate()

    //创建StreamingContext对象
    val ssc = new StreamingContext(spark.sparkContext,Seconds(4))

    // 创建一个可变的队列对象
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //创建DStream监控队列流
    val ds = ssc.queueStream(rddQueue)

    //处理DStream的数据——业务逻辑
    val result = ds.map((1,_)).reduceByKey(_+_)

    result.print()

    ssc.start()

    //生成数据
    for (i <- 1 to 1000){
      val r = ssc.sparkContext.makeRDD((1 to i+1))
      r.collect()
      rddQueue += r
      Thread.sleep(3000)
    }

    ssc.awaitTermination()
  }


}

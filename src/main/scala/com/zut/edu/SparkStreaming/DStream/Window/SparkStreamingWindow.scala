package com.zut.edu.SparkStreaming.DStream.Window

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWindow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("update")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    //设置检查点目录
    ssc.checkpoint("E:\\Desktop\\data")
    //获取DStream
    val dStream = ssc.socketTextStream("hadoop0",1111)
    //WordCount
    val words = dStream.map((_,1))
    words.reduceByKey(_+_).print()

    val window = words.reduceByKeyAndWindow(
      (a:Int,b:Int  ) => a+b, //reduce:一个窗口中的V做累加
      Seconds(20),  //窗口的时间间隔，必须为Stream时间间隔的整数倍，4
      Seconds(10)   //滑动的步长，2
    )

    window.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

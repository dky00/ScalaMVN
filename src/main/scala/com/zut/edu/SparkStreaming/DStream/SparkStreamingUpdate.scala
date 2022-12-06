package com.zut.edu.SparkStreaming.DStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


//有状态转换
object SparkStreamingUpdate {
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
    //状态更新
    val state = words.updateStateByKey[Int]((values:Seq[Int],state:Option[Int]) => {
      //value当前批次的单词频数
      //state为之前批次单词频数
      val currentCount = values.foldLeft(0)(_+_)
      //getOrElse如果有值则获取值，如果没有则使用默认值
      val newState = currentCount + state.getOrElse(0)
      //返回新的状态
      Some(newState)
    })

    state.print()

    ssc.start()
    ssc.awaitTermination()


  }

}

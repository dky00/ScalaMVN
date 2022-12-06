package com.zut.edu.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("file")
      .getOrCreate()

    //创建StreamingContext对象
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))

    val ds = ssc.textFileStream("hdfs://hadoop0:9000//data/word/in")
    val result = ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    //启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }

}

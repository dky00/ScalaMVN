package com.zut.edu.SparkStreaming.DStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


//自定义转换
object SparkStreamingTransform {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("transform")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))

    //从Socket接收数据
    val dStream = ssc.socketTextStream("hadoop0",1111)
    val words = dStream.flatMap(_.split(" ")).map((_,1))

    //创建黑名单
    val blackList = ssc.sparkContext.parallelize(List(",","?","!",".")).map((_,true))

    val need = words.transform(rdd => {
      val leftRDD = rdd.leftOuterJoin(blackList)
      val result = leftRDD.filter(t => {
        if(t._2._2.isEmpty){
          true
        }else{
          false
        }
      })
      result.map(t => (t._1,1))
    })

    need.reduceByKey(_+_).print

    ssc.start()
    ssc.awaitTermination()





  }
}

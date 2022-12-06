package com.zut.edu.Spark.ELasticsreach

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object insert {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("es.index.auto.create","true")
      .config("es.nodes","localhost:9200")
      .appName("ES")
      .master("local[*]")
      .getOrCreate()
    val sc = ss.sparkContext

    //读取ES数据
    val rdd = sc.makeRDD(List(
      Map("name" -> "待兼诗歌剧","sex" -> "女","age" -> 20),
        Map("name" -> "玉藻十字","sex" -> "女","age" -> 19),
        Map("name" -> "小栗帽","sex" -> "女","age" -> 21)
    ))

    EsSpark.saveToEs(rdd,"user/_doc")

    sc.stop()
  }

}

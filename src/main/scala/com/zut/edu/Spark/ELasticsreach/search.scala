package com.zut.edu.Spark.ELasticsreach

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object search {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config("es.index.auto.create","true")
      .config("es.nodes","localhost:9200")
      .appName("ES")
      .master("local[*]")
      .getOrCreate()
    val sc = ss.sparkContext

    //读取ES数据
    val rdd = EsSpark.esRDD(sc,"user/")
    rdd.foreach(line =>{
      println("ID:" + line._1)
      for(ele <- line._2){
        println(ele._1 + ":" + ele._2)
      }
    })

    sc.stop()

  }

}

package com.zut.edu.Spark.MySQL

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager


object jdbc {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().appName("MySQL").master("local[*]").getOrCreate()
    val sc = ss.sparkContext

    //MySQL参数
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark_sql_test?characterEncoding=utf-8&serverTimezone=PRC"

    val userName = "root"
    val password = "dky1510"

    //创建RDD对象
    val result: JdbcRDD[(Int, String, Int, Int)] = new JdbcRDD(
      sc,
      () => {
        DriverManager.getConnection(url, userName, password)
      },
      sql = "select * from user where id >= ? and id <= ?",
      0, 100,
      1,
      r => (r.getInt(1), r.getString(2), r.getInt(3), r.getInt(4))
    )
    result.foreach(println)

    sc.stop()
    ss.stop()
  }

}

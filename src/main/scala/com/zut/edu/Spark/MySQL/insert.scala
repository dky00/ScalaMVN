package com.zut.edu.Spark.MySQL

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager

object insert {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("MySQL").master("local[*]").getOrCreate()
    val sc = ss.sparkContext

    //MySQL参数
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark_sql_test?characterEncoding=utf-8&serverTimezone=PRC"
    val userName = "root"
    val password = "dky1510"

    //创建RDD对象
    val rdd = sc.makeRDD(List(
      ("待兼诗歌剧",18,159931816)
    ))
    /**
      rdd.foreach{
      case (name,age,phone) => {
        val conn = DriverManager.getConnection(url,userName,password)
        val sql = "insert into user(name,age,phone) values (?,?,?)"
        val statement = conn.prepareStatement(sql)
        statement.setString(1,name)
        statement.setInt(2,age)
        statement.setInt(3,phone)
        statement.executeUpdate()
        statement.close()
        //关闭连接
        conn.close()
        }
      }
     */
    rdd.foreachPartition(its =>{
      val conn = DriverManager.getConnection(url,userName,password)
      val sql = "insert into user(name,age,phone) values (?,?,?)"
      val statement = conn.prepareStatement(sql)
      for(ele <- its){
        statement.setString(1,ele._1)
        statement.setInt(2,ele._2)
        statement.setInt(3,ele._3)
        statement.executeUpdate()
      }
      statement.close()
      conn.close()
    })

    sc.stop()
    ss.stop()
  }

}

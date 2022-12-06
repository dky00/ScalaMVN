package com.zut.edu.Spark.movie

import org.apache.spark.{SparkConf, SparkContext}

object MoviesScore {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MoviesScore").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val moviesRDD = sc.textFile("E:\\Desktop\\ml-1m\\movies.dat")
    val ratingsRDD = sc.textFile("E:\\Desktop\\ml-1m\\ratings.dat")

 //   val ratings = ratingsRDD.map(l => (l.split("::")(1),l.split("::")(2).toDouble)).groupByKey().map(x => (x._1,x._2))
    println("所有电影中平均得分大于4分的电影:")
    val movieInfo = moviesRDD.map(_.split("::")).map(x=>(x(0),x(1))).cache()
    val ratings = ratingsRDD.map(_.split("::")).map(x=>(x(1),x(2))).cache()
    val moviesAndRatings = ratings.map(x=>(x._1,(x._2.toDouble,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    val avgRatings = moviesAndRatings.map(x=>(x._1,x._2._1.toDouble/x._2._2))
    val  avg = avgRatings.join(movieInfo).map(item=>(item._2._1,item._2._2))
      .filter(x => x._1 > 4.0).sortByKey(false).map(x => (x._2,x._1))/*.sortByKey(false)
      .foreach(record=>printf(" %s \t评分为: %.2f \n",record._2,record._1))*/
    avg.foreach(println)
    /*
    avgRatings.join(movieInfo).map(item=>(item._2._1,item._2._2))
              .sortByKey(false).take(10)
              .foreach(record=>println(record._2+"评分为:"+record._1))
              */
    avg.saveAsTextFile("E:\\Desktop\\ratings" )

  }

}

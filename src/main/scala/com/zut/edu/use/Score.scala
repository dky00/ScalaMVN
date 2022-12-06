package com.zut.edu.use

object Score {
  def main(args: Array[String]): Unit = {
    val inputFile = scala.io.Source.fromFile("src/main/resources/score.txt")
    //inputFile.getLines.foreach(l => println(l))
    val data = inputFile.getLines().map(_.split("\t")).toList
    val head = data.head
    //head.foreach(println(_))
    val scores = data.tail


   // val score = () => {for(i <- 2 to head.length-1 )  scores.map(l => l(i).toDouble)}

    for(i <- 2 to head.length-1 ){
      val score1 = scores.map(l => l(i).toDouble)
      printf("%s : %.2f   %.2f   %.2f\n",head(i),score1.sum/score1.length,score1.min,score1.max)
    }

    println("\nMale`s Score")
    for(i <- 2 to head.length-1 ){
      val score2 = scores.filter(_(1).equals("male")).map(_(i).toDouble)
      printf("%s : %.2f   %.2f   %.2f\n",head(i),score2.sum/score2.length,score2.min,score2.max)
    }

    println("\nFemale`s Score")
    for(i <- 2 to head.length-1 ){
      val score3 = scores.filter(_(1).equals("female")).map(_(i).toDouble)
      printf("%s : %.2f   %.2f   %.2f\n",head(i),score3.sum/score3.length,score3.min,score3.max)
    }





  }

}

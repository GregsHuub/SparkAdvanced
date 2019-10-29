package net.atos

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD

object SparkFriend {

  val fakeFriendsCSV = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\SparkScalaExamplesExercises\\fakefriends.csv"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val friendsLines: RDD[String] = sc.textFile(fakeFriendsCSV)
    //    friendsLines.foreach(println)



    val splitedRdd = friendsLines
      .map(makeSplitForAge)
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect
      .toSeq
      .sortBy(_._1)
//      .foreach(println)



    val averageNumOfFriendsByName = friendsLines
      .map(makeSplitForName)
      .mapValues( x => (x,1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues( x=> x._1 / x._2)
      .collect
      .toSeq
      .sortBy(_._1)

    averageNumOfFriendsByName.foreach(println)



  }


  def makeSplitForName(x:String): (String, Int) = {
    val split = x.split(",")
    val name = split(1).toString
    val numFriends = split(3).toInt
    (name, numFriends)
  }

  def makeSplitForAge(x: String): (Int, Int) = {
    val split = x.split(",")
    val age = split(2).toInt
    val numFriends = split(3).toInt
    (age, numFriends)
  }



//  average  number of friends by first name

}

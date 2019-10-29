package net.atos

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines: RDD[String] = sc.textFile("C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\DANE\\ml-100k\\ml-100k\\u.data")

    val mappedLine: RDD[String] = lines.map(x => x.toString.split("\t")(2))
    val countedLine: collection.Map[String, Long] = mappedLine.countByValue()
//    val sortedLines = countedLine.toSeq.sortBy(_._1).foreach(println)
    println("END OF TEST")

    val crimeRecords  = sc.textFile("C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\materialy\\SacramentocrimeJanuary2006.csv")


    def parseLineByCrime(line:String) = {
      val fields = line.split(",")
      val crimeDscr: String = fields(5)
      val time: String = fields(0)
      (time, crimeDscr)
    }
    val crimeRdd = crimeRecords.map(parseLineByCrime)
    val sumByCrimeDescription: RDD[(String, (String, Int))] = crimeRdd.mapValues(x => (x,1))reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val sorted = sumByCrimeDescription.sortBy(_._1)

    println("CRIME TEST- address, description")
//    sorted.foreach(println)

    val agesAndFriedsLine = sc.textFile("C:\\Users\\a769324\\Desktop\\ageAndfriends.csv")

    def parseFriends(line:String) ={
      val fields = line.split(";")
      val age = fields(0)
      val numberOfFriends = fields(1)
      (age,numberOfFriends)
    }


    println("Check rows")
    val rdd: RDD[(String, String)] = agesAndFriedsLine.map(parseFriends)
    val sumRddFriends = rdd
    sumRddFriends.foreach(println)







    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results: collection.Map[String, Long] = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults: Seq[(String, Long)] = results.toSeq.sortBy(_._1)

//    results.foreach(println)





//    println("sorted result")
//    sortedResults.foreach(println)
  }
}

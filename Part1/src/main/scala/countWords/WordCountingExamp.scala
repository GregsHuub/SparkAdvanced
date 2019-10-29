package countWords

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountingExamp {

  val FILE = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\SparkScalaExamplesExercises\\book.txt"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "Count Words from book")

    val rddFile = sc.textFile(FILE)

    val splited = rddFile.flatMap(x => x.split("\\W+"))

    val lowerCase = splited.map(x => x.toLowerCase())

    val countWords = lowerCase.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    val sorted = countWords.map(x => (x._2, x._1)).sortByKey()


    for (result <- sorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }


  }
}

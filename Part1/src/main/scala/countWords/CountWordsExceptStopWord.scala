package countWords

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CountWordsExceptStopWord {

  val FILE_LOCATION = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\SparkScalaExamplesExercises\\book.txt"
  val STOP_DICTIONARY = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\DANE\\ml-100k\\stopwords.txt"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "Count except stoping words")

    val book = sc.textFile(FILE_LOCATION)
    val dictionary = sc.textFile(STOP_DICTIONARY)
    val collectionOfDictionary: Array[String] = dictionary.collect


    val splited = book.flatMap(x => x.split("\\W+")).map(x => x.toLowerCase())

    val countWords = splited.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    val changePlaces: RDD[(Int, String)] = countWords.map(x => (x._2, x._1)).sortByKey()


    val afterFilter = changePlaces
      .filter(x => !collectionOfDictionary.contains(x._2))

    afterFilter.foreach(println)

  }
}

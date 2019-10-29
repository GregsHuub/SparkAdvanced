package net.atos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MinimumWeatherTemp {

  val weatherLink = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\SparkScalaExamplesExercises\\1800.csv"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Min Temp ")
    val rddWeahter = sc.textFile(weatherLink)

    val onylyMinTemp: RDD[(String, String, Int)] = rddWeahter.map(splitAndMinimum)
      .filter(x => x._2.equals("TMIN"))

    val onlyStationAndType: RDD[(String, Int)] = onylyMinTemp.map(x => (x._1, x._3))
    val consolidatedSameStations = onlyStationAndType.reduceByKey((x, y) => scala.math.min(x, y))

    consolidatedSameStations.foreach(println)

  }

  def splitAndMinimum(x: String) = {
    val splited = x.split(",")
    val stationId = splited(1)
    val entryType = splited(2).toString
    val minTemp = splited(3).toInt
    (stationId, entryType, minTemp)
  }


}

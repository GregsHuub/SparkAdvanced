package net.atos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MaxWeatherTemp {

  //  private val LOGGER = LoggerFactory.getLogger(getClass.getName)


  val fileLocation = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\SparkScalaExamplesExercises\\1800.csv"

  def main(args: Array[String]): Unit = {

    //    LOGGER.info("TEST logging")
    Logger.getLogger("org").setLevel(Level.ERROR)
    //    val session = SparkSession.builder().appName("Max-Temp").master("local[*]").getOrCreate()
    val sc = new SparkContext("local[*]", "Min Temp ")

    // DO PRACY NA KLASTRZE
    //    val hdfs = FileSystem.get(session.sparkContext.hadoopConfiguration)

    //    DO PRACY LOKALNEJ/CWICZEN
    //    val hdfsLocal = FileSystem.get(new Configuration())

    //    val dataFrame: DataFrame = session.read.csv(fileLocation)

    //    dataFrame.printSchema()
    //    dataFrame.show(false)
    //    dataFrame.show()


    val rddFile = sc.textFile(fileLocation)
    val splited = rddFile.map(splitDates).filter(x => x._2.equals("TMAX"))
    val cutTHeList = splited.map(x => (x._1, x._3))
    val onlyMaxValue = cutTHeList.reduceByKey((x, y) => scala.math.max(x, y))
    val result: Array[(String, Int)] = onlyMaxValue.collect()

    val precipitationCheck = sc.textFile(fileLocation).map(checkPrecipitation).filter(x => x._2.contains("PRCP"))
    val shortedList = precipitationCheck.map(x =>( x._1, x._3))
    val reducec: RDD[(String, Int)] = shortedList.reduceByKey((x, y) => scala.math.max(x,y))
    val newArray: Array[(String, Int)] = reducec.collect
    val sorted = newArray.sortBy(_._2)
    val maxValue = sorted(sorted.size -1)
    println(maxValue)





//    for(results <- result.sorted){
//      val station = results._1
//      val temp = results._2
//      println(s"$station max temp: $temp")
//    }

  }

  def splitDates(x: String) = {
    val splited = x.split(",")
    val stationId = splited(1)
    val entryType = splited(2).toString
    val minTemp = splited(3).toInt
    (stationId, entryType, minTemp)
  }
  def checkPrecipitation(x:String) = {
    val splitArg = x.split(",")
    val day = splitArg(1).toString
    val typeOf = splitArg(2).toString
    val value = splitArg(3).toInt
    (day, typeOf, value)
  }

}

package popularMovies

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MostPopMov {

  val FILE = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\DANE\\ml-100k\\ml-100k\\u.data"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val sc = new SparkContext("local", "popularMovies")

    val fileToRdd2 = sc.textFile(FILE)

    val movies: RDD[(Int, Int)] = fileToRdd2.map(x => (x.split("\t")(1).toInt, 1))

    val reduceFrank = movies.reduceByKey((x,y) => x + y)
    reduceFrank.sortByKey().foreach(println)

  }


}

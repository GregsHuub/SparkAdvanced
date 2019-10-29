package broadcastUseNameMovies

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.io.{Codec, Source}

object BroadcastUse {
  val FILEUSOURCE = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\DANE\\ml-100k\\ml-100k\\u.item"
  val FILEDATA = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\DANE\\ml-100k\\ml-100k\\u.data"

  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines: Iterator[String] = Source.fromFile(FILEUSOURCE).getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "popular movies with names")

    val nameDict: Broadcast[Map[Int, String]] = sc.broadcast(loadMovieNames)
    val lines = sc.textFile(FILEDATA)

    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey()
      .map(x => (nameDict.value(x._2), x._1))
      .collect()
      .foreach(println)

  }
}

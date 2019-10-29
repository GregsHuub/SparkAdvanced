package originalLectureFiles

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String): Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local", "MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile("../Part1/src/resources/marvelDetails/Marvel-names.txt")
    val namesRdd: RDD[(Int, String)] = names.flatMap(parseNames)
    val mappedNames = namesRdd.collect()

    // Load up the superhero co-apperarance data
    val lines = sc.textFile("C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\SparkInScala\\Part1\\src\\resources\\marvelDetails\\Marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences)

    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey((x, y) => x + y)

    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))

    val sortMax = flipped.sortByKey(false).collect

    val tenFirstPositions = sortMax.slice(0, 9)

    val tenLastPositions = sortMax.slice((sortMax.length - 10), (sortMax.length - 1))


    // Find the max # of connections
    val mostPopular: (Int, Int) = flipped.max()

    for (x <- tenFirstPositions.indices) {
      val mostPopularNameFirst = namesRdd.lookup(tenFirstPositions(x)._2)
      println(s"$mostPopularNameFirst with ${tenFirstPositions(x)._1}")

    }
    for( x <- tenLastPositions.indices) {
      val mostPopularNameLast = namesRdd.lookup(tenLastPositions(x)._2)
      println(s"$mostPopularNameLast with ${tenLastPositions(x)._1}")

    }
    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
//    val mostPopularName = namesRdd.lookup(mostPopular._2)


    // Print out our answer!
    //    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
  }

}

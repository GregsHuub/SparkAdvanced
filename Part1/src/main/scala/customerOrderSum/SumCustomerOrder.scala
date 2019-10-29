package customerOrderSum

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SumCustomerOrder {

  val FILE_LOCATION = "C:\\Users\\a769324\\Desktop\\Kurs SPARK z UDEMY\\SparkScalaExamplesExercises\\customer-orders.csv"
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "Sum up order")

    val rddFile = sc.textFile(FILE_LOCATION)
    val splited: RDD[(Int, Double)] = rddFile.map(splitFile)
    val totalCustomerSimpleSolution = splited.reduceByKey((x,y) => x + y)
//    val totalCustomer: RDD[(Int, (Double, Int))] = splited
//    .mapValues(x => (x,1))reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//    val finalVersion = totalCustomer.mapValues(x => (x._1))
    val sortByCustomer = totalCustomerSimpleSolution.sortBy(_._1)
    val sortByAmountSpent = totalCustomerSimpleSolution.sortBy(_._2, false)

    for(list <- sortByAmountSpent){
      val id = list._1
      val value = list._2
      println(s"customer: $id = value: $value")
    }


  }

  def splitFile(x:String) = {
    val content = x.split(",")
    val customerId = content(0).toInt
    val value = content(2).toDouble
    (customerId, value)
  }


}

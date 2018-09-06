package jyb.zju

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AppSpread {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app spread")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val job = args(0).toInt
    job match {
      case 1 => DataFormat(sc)
      case 2 => {
        // Array(15, 20, 25, 30).foreach(sec => CallGraph(sc, sec))
        // Array(15, 20, 25, 30).foreach(sec => MaxComponent(sc, sec))
        Array(15, 20, 25, 30).foreach(sec => {
          for (knn <- 1 to 15) AppSimGraph(sc, knn, sec)
        })
        // AppSimGraph(sc, 1, 15)
      }
      case 3 => {
        val eta = args(1).toDouble
        println("[INFO] cross validation on network types and dimension size")
        MyCrossValidation(sc, 10, eta)
      }
    }
  }

}

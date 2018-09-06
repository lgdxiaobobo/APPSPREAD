package jyb.zju

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.io.Source

import java.io.Serializable
import java.text.SimpleDateFormat

case class Item(user: String, app: String, day: String);

private class DataFormat() extends Serializable {

  def loadHostToApp(sc: SparkContext): Broadcast[Map[String, String]] = {
    println("[INFO] load host to app dict")
    val file = Source.fromFile("config/app_index_to_host.csv")
    val line = file.getLines()
    val dict = line.map(p => p.split(','))
                   .map(ps => (ps(0), ps(1)))
                   .toMap
    println("[INFO] with %d corresponding relation".format(dict.size))
    sc.broadcast(dict)
  }

  def matchApp(dict: Map[String, String]): Array[String] => Array[String] = {
    ps: Array[String] => {
      val host = ps(2)
      val url  = ps(3)
      val app  = dict.getOrElse(host, dict.getOrElse(url, ""))
      ps.take(2) :+ app
    }
  }

  def secToDay(ps: Array[String]): Array[String] = {
    val sec = ps(1).toInt
    val milSec = sec * 1000L
    val dateFmt= new SimpleDateFormat("yyyyMMdd")
    val date = dateFmt.format(milSec)
    Array(ps(0), date, ps(2))
  }

  def validDate(ps: Array[String]): Boolean = {
    ps(1) < "20170301"
  }

  def formatData(sc: SparkContext, bDict: Broadcast[Map[String, String]], basePath: String, partNum: Int, resPath: String): Boolean = {
    val size = partNum - 1
    val idx = Array(1, 0, 4, 10)
    for (part <- 0 to size){
      println("[INFO] formatted with " + basePath.format(part))
      val other = sc.textFile(basePath.format(part))
      val userTimeApp = other.map(p => p.split('|'))
                             .map(ps => idx.map(ps.apply))
                             .map(matchApp(bDict.value))
                             .filter(_.last != "")
      println("[INFO] %d tuples remains after matching".format(userTimeApp.count()))
      val userNum = userTimeApp.map(_.head).distinct().count()
      val pkgNum = userTimeApp.map(_.last).distinct().count()
      println("[INFO] %d users and %d apps".format(userNum, pkgNum))
      val userDayApp = userTimeApp.map(secToDay).filter(validDate)
      val tupleCnt = userDayApp.map(ps => ps.mkString("|"))
                               .map(x => (x, 1)).reduceByKey(_ + _)
      val userDayAppCnt = tupleCnt.map{case (tuple, cnt) => tuple.split('|') :+ cnt.toString}
      userDayAppCnt.map(ps => ps.mkString("|")).repartition(8).saveAsTextFile(resPath.format(part))
    }
    true
  }

  def formatData(sc: SparkContext, bDict: Broadcast[Map[String, String]]): Unit = {
    println("[INFO] formatted data from other")
    val otherBasePath = "/warehouse/other/other_md5_%d"
    val webBasePath = "/warehouse/web/web_md5_%d"
    val otherResPath = "/appSpreadv2/other/part%d"
    val webResPath = "/appSpreadv2/web/part%d"
    formatData(sc, bDict, otherBasePath, 64, otherResPath)
    formatData(sc, bDict, webBasePath, 4, webResPath)
  }

  def unionData(sc: SparkContext): Unit = {
    println("[INFO] uniform app usage data from other and web")
    val data = sc.textFile("/appSpreadv2/{other,web}/*").map(_.split('|'))

    println("[INFO] %d tuples totally".format(data.count()))

    val userNum = data.map(_.apply(0)).distinct().count()
    val pkgNum = data.map(_.apply(2)).distinct().count()
    println("[INFO] %d users and %d pkgs totally".format(userNum, pkgNum))

    val careScore = data.map(ps => (ps(0), Set(ps(2))))
                        .reduceByKey(_ union _).map(_._2.size).mean()
    println("[INFO] at average a single user used %f apps".format(careScore))

    val union = data.map(ps => (ps.dropRight(1).mkString("|"), ps.last.toInt))
                    .reduceByKey(_ + _)
    union.map{case (tuple, cnt) => tuple.split('|') :+ cnt.toString()}
         .map(_.mkString("|")).repartition(64)
         .saveAsTextFile("/appSpreadv2/appUsage")
  }
}

object DataFormat {
  def apply(sc: SparkContext): Unit = {
    val model = new DataFormat()
    val bDict = model.loadHostToApp(sc)
    model.formatData(sc, bDict)
    model.unionData(sc)
  }
}

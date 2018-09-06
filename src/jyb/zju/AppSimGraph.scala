package jyb.zju

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.Serializable

case class Node(name: String, score: Double){
  def smaller(other: Node): Boolean = score < other.score
}

private class AppSimGraph(d: Int) extends Serializable {

  private def jaccScore(a: Set[String], b: Set[String]): Double = {
    val aUnionB = a.union(b)
    val aInterB = a.intersect(b)
    aInterB.size * 1.0 / aUnionB.size
  }

  private def cosinScore(a: Vector, b: Vector) = {
    a.dot(b) / (a.norm() * b.norm())
  }

  private def obtainLinks(src: String, pkgs: Set[String], info: Array[(String, Set[String])]): (String, Array[Node]) = {
    val neighbour = new TopNList(this.d)
    info.foreach{case (dst, used) =>
      if (!src.equals(dst)){
        val score = this.jaccScore(pkgs, used)
        neighbour.add(Node(dst, score))
      }
    }
    (src, neighbour.topN)
  }

  private def obtainLinks(i: String, ui: Vector, info: Map[String, Vector]): (String, Array[Node]) = {
    val neighbour = new TopNList(this.d)
    info.toArray.foreach{case (l, ul) =>
      if (!i.equals(l)){
        val score = this.cosinScore(ui, ul)
        neighbour.add(Node(l, score))
      }
    }
    (i, neighbour.topN)
  }

  def directGraph(status: RDD[(String, Set[String])]): RDD[String] = {
    val sc = status.context
    val userToPkgs = status.collect()
    val infoBD = sc.broadcast(userToPkgs)
    val knn = status.map{case (src, pkgs) => obtainLinks(src, pkgs, infoBD.value)}
    knn.flatMap{case (src, neighbors) => neighbors.map(dst => (src, dst.name))}
       .flatMap{case (src, dst) => Array((src, dst), (dst, src))}
       .map{case (src, dst) => Array(src, dst).mkString(",")}
       .distinct()
  }

  def improvedGraph(status: RDD[(String, Set[String])], userFactor: Map[String, Vector]): RDD[String] = {
    val sc = status.context
    val fuBD = sc.broadcast(userFactor)
    val knn = status.map{case (i, _) => obtainLinks(i, fuBD.value(i), fuBD.value)}
    knn.flatMap{case (src, neighbors) => neighbors.map(dst => (src, dst.name))}
      .flatMap{case (src, dst) => Array((src, dst), (dst, src))}
      .map{case (src, dst) => Array(src, dst).mkString(",")}
      .distinct()
  }
}

object AppSimGraph {
  def loadUsedApps(sc: SparkContext): RDD[(String, Set[String])] = {
    val usedPkgs = sc.textFile("/appSpreadv2/appUsage").map(_.split('|'))
    val usersToPkgs = usedPkgs.map(ps => (ps(0), Set(ps(2)))).reduceByKey(_ union _)
    usersToPkgs
  }

  def isFileExist(sc: SparkContext, filename: String): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(filename))
    exists
  }

  def loadInvolvedInfo(sc: SparkContext, sec: Int): RDD[(String, Set[String])] = {
    val filename = "/appSpreadv2/tmp/pkgs_with_%d".format(sec)
    val setUserPath = "/appSpreadv2/max_components_%d".format(sec)
    if (!isFileExist(sc, filename)){
      val usedAppsList = this.loadUsedApps(sc)
      val value1 = usedAppsList.map(_._2.size).mean()
      println("[INFO] every users used %f apps at average".format(value1))

      val maxCCUsers = sc.textFile(setUserPath).collect().toSet
      println("[INFO] %d users in max connected component".format(maxCCUsers.size))

      val usersBD = sc.broadcast(maxCCUsers)
      val temp = usedAppsList.filter(p => usersBD.value.contains(p._1))

      val pkgCnt = temp.flatMap(_._2).map(x => (x, 1)).reduceByKey(_ + _)
      val pkgGt10 = pkgCnt.filter(_._2 > 10).map(_._1).collect().toSet
      println("[INFO] %d pkgs involved and %d pkgs are used more than 10 users".format(pkgCnt.count(), pkgGt10.size))

      val pkgsBD = sc.broadcast(pkgGt10)
      val validList = temp.map(p => (p._1, p._2.intersect(pkgsBD.value))).filter(_._2.nonEmpty)
      println("[INFO] %d users involfed".format(validList.count()))

      validList.map { case (src, usedPkgs) => src + "|" + usedPkgs.mkString(",") }
        .repartition(8).saveAsTextFile(filename)
    }

    sc.textFile(filename).map(_.split('|')).map(ps => (ps(0), ps(1).split(',').toSet))
  }

  def loadLatentFactor(sc: SparkContext, sec: Int, status: RDD[(String, Set[String])], d: Int): Map[String, Vector] = {
    val filename = "/appSpreadv2/tmp/lfm_%d/lfm_with_%d".format(d, sec)
    if (!isFileExist(sc, filename)){
      val model = LFM(d)
      val factors = model.latentFactor(status, study_rate = 0.0001)
      sc.parallelize(factors.toArray.map{case (i, ui) => i +: ui.getValue().map(_.toString)}.map(_.mkString(",")))
        .repartition(8).saveAsTextFile(filename)
    }
    sc.textFile(filename).map(_.split(','))
      .map(ps => (ps(0), ps.tail.map(_.toDouble)))
      .map{case (i, ui) => (i, new Vector(ui))}
      .collect().toMap
  }

  def apply(sc: SparkContext, d: Int, sec: Int): Unit = {
    println("[INFO] find knn with size-%d more than %d sec".format(d, sec))
    val validList = loadInvolvedInfo(sc, sec)
    val model = new AppSimGraph(d)

    val  directName = "/appSpreadv2/graph/direct_graph_%dNN_%dsec".format(d, sec)
    if (!isFileExist(sc, directName)) {
      val gd = model.directGraph(validList)
      println("[INFO] %d edges in directed graph with %d-nn and more than %d sec".format(gd.count(), d, sec))
      gd.repartition(8).saveAsTextFile(directName)
    }

    val improvedName = "/appSpreadv2/graph/improved_graph_%dNN_%dsec".format(d, sec)
    if (!isFileExist(sc, improvedName)) {
      val meanCnt = validList.map(_._2.size).mean()
      println("[INFO] every users use %f apps".format(meanCnt))
      val rank = 5
      val lfm = loadLatentFactor(sc, sec, validList, rank)
      println("[INFO] finish lfm with rank-%d at %d sec dataset".format(rank, sec))
      val ga = model.improvedGraph(validList, lfm)
      println("[INFO] %d edges in improved graph".format(ga.count()))
      ga.repartition(8).saveAsTextFile("/appSpreadv2/graph/improved_graph_%dNN_%dsec".format(d, sec))
    }
  }
}

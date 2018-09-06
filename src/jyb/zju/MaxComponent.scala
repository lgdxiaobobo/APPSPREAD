package jyb.zju

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.Serializable

private class MaxComponent(minSec: Int) extends Serializable {
  private val graphPath = "/appSpreadv2/graph_based_logs_%d".format(minSec)
  private val maxCCPath = "/appSpreadv2/max_components_%d".format(minSec)

  def min(a: Long, b: Long): Long = if (a < b) a else b

  def matchCCID(dict: Map[String, Long]): Array[String] => (String, String, Long) = {
    ps => {
      val src = ps(0)
      val dst = ps(1)
      val srcId = dict(src)
      val dstId = dict(dst)
      (src, dst, min(srcId, dstId))
    }
  }

  def generateMaxComponent(sc: SparkContext): RDD[(Long, Array[String])] = {
    val graph = sc.textFile(graphPath).map(_.split(','))
    val nodes = graph.flatMap(ps => ps).distinct()
    println("[INFO] totally %d nodes in call graph".format(nodes.count()))
    var ccDict= nodes.zipWithIndex().collect().toMap
    var flag = true
    while (flag){
      val dictBD = sc.broadcast(ccDict)
      val status = graph.map(matchCCID(dictBD.value))
      val update = status.flatMap{case (src, dst, ccId) => Array((src, ccId), (dst, ccId))}
        .reduceByKey(min)
      val differ = update.filter{case (node, ccId) => dictBD.value(node) != ccId}
      flag = differ.count() > 0
      println("[INFO] %d different remains".format(differ.count()))
      ccDict = update.collect().toMap
    }
    val dictBD = sc.broadcast(ccDict)
    val ccToNodes = nodes.map(x => (dictBD.value(x), Array(x)))
      .reduceByKey(_ ++ _).map(_._2).zipWithIndex()
      .map(p => (p._2, p._1)).sortBy(_._2.size, false)
    val ccStatic = ccToNodes.map{case (ccId, nodes) => (ccId, nodes.size)}
    println("[INFO] there are %d connected components".format(ccStatic.count()))
    println("[INFO] 10 most components info")
    ccStatic.take(10).foreach{case (ccId, cnt) => println("%d-th component with %d nodes".format(ccId, cnt))}
    ccToNodes
  }

  def saveMaxComponent(ccToNodes: RDD[(Long, Array[String])]): Unit = {
    val maxCCID = ccToNodes.map(_._1).first()
    val maxCCNodes = ccToNodes.filter(_._1 == maxCCID).map(_._2)
    maxCCNodes.flatMap(x => x).repartition(8).saveAsTextFile(maxCCPath)
  }
}

object MaxComponent {

  def apply(sc: SparkContext, minSec: Int): Unit = {
    val model = new MaxComponent(minSec)
    val idToNodes = model.generateMaxComponent(sc)
    model.saveMaxComponent(idToNodes)
    // save graph in max component
    val nodes = sc.textFile("/appSpreadv2/max_components_%d".format(minSec))
    val rawGraph = sc.textFile("/appSpreadv2/graph_based_logs_%d"
                     .format(minSec)).map(_.split(','))
                     .map(ps => (ps(0), ps(1)))
    val valid = sc.broadcast(nodes.collect().toSet)
    val graphInCC = rawGraph.filter{case (src, dst) => valid.value.contains(src) && valid.value.contains(dst)}
    println("[INFO] there are %d relations remains".format(graphInCC.count()))
    graphInCC.map{case (src, dst) => src + "," + dst}.repartition(8).saveAsTextFile("/appSpreadv2/call_graph_%d".format(minSec))
  }
}

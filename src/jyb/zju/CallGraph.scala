package jyb.zju

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import java.io.Serializable

case class VItem(sec: Int, src: String, dst: String, dur: Int)
case class SItem(sec: Int, src: String, dst: String, flag: Int)

private class CallGraph(minSec: Int) extends Serializable {

  private val callLogPath = "/warehouse/voice/*"
  private val smsLogPath = "/warehouse/sms/*"

  // load valid users
  def loadValidUsers(sc: SparkContext): Broadcast[Set[String]] = {
    println("[INFO] load valid users")
    val data = sc.textFile("/warehouse/other_users")
    sc.broadcast(data.collect().toSet)
  }

  def anaVoice(valid: Broadcast[Set[String]], data: RDD[VItem]): RDD[String] = {
    // filter relation according to valid users
    val filter1 = data.filter(p => valid.value.contains(p.src) && valid.value.contains(p.dst))
    // duration is more than 30 sec
    println("[INFO] voice records duration more than %d seconds".format(minSec))
    val filter2 = filter1.filter(p => p.dur >= minSec * 1000)
    // print info
    val relation = filter2.flatMap(p => Array((p.src, p.dst), (p.dst, p.src)))
      .map { case (src, dst) => src + "," + dst }.distinct()
    println("[INFO] there are %d relations found by call-logs".format(relation.count()))
    relation
  }

  def anaSms(valid: Broadcast[Set[String]], data: RDD[SItem]): RDD[String] = {
    // filter relation according to valid users
    val filter1 = data.filter(p => valid.value.contains(p.src) && valid.value.contains(p.dst))
    // mutual relation remains (send and recieve)
    val filter2 = filter1.flatMap(p => Array((p.src + "," + p.dst, Set(p.flag)), (p.dst + "," + p.src, Set(p.flag))))
      .reduceByKey(_.union(_)).filter(_._2.size == 2)
    // print info
    val relation = filter2.map(_._1)
    println("[INFO] there are %d relations found by sms-logs".format(relation.count()))
    relation
  }

  def generateCallGraph(sc: SparkContext, valid: Broadcast[Set[String]]): RDD[String] = {
    val voice = sc.textFile(this.callLogPath).map(_.split('|'))
      .map(ps => VItem(ps(0).toInt, ps(1), ps(5), ps(8).toInt))
    println("[INFO] generate call graph based call-log")
    val vRelation = anaVoice(valid, voice)
    val sms = sc.textFile(this.smsLogPath).map(_.split('|'))
      .map(ps => SItem(ps(0).toInt, ps(1), ps(5), ps(7).toInt))
    println("[INFO] generate call graph based sms-log")
    val sRelation = anaSms(valid, sms)
    val relation = vRelation.union(sRelation).distinct()
    println("[INFO] after merged there are %d relations remains".format(relation.count()))
    // vRelation.repartition(8).saveAsTextFile("/appSpreadv2/graph_based_call_logs")
    // sRelation.repartition(8).saveAsTextFile("/appSpreadv2/graph_based_sms_logs")
    // relation.repartition(8).saveAsTextFile("/appSpreadv2/graph_based_logs")
    relation
  }
}

object CallGraph {
  def apply(sc: SparkContext, minSec: Int): Unit = {
    val model = new CallGraph(minSec)
    val validUsers = model.loadValidUsers(sc)
    val callGraph = model.generateCallGraph(sc, validUsers)
    callGraph.repartition(8).saveAsTextFile("/appSpreadv2/graph_based_logs_%d".format(minSec))
  }
}

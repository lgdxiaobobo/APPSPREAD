package jyb.zju

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.math.{sqrt, abs}

import java.io.Serializable

case class Value(i: Int, j: Int, t: String, m0: Int, m1: Int, ni: Array[(Int, Int)]){
  override def toString(): String = {
    val head = "%d,%d,%s,%d,%d".format(i, j, t, m0, m1)
    val tail = ni.map{case (l, ml0) => "%d#%d".format(l, ml0)}.mkString("|")
    head + "," + tail
  }
}

case class Param(alpha: Double, beta: Double){
  def add(other: Param): Param = {
    Param(this.alpha + other.alpha, this.beta + other.beta)
  }
}

private class MyCrossValidation(fold: Int, rng: Random, d: Int, eta: Double) extends Serializable {

  private def takeSample(idx: Array[String], n: Int, rand: Random): Array[String] = {
    if (n <= 0 || idx.length == 0) Array[String]()
    else{
      val len = idx.length
      val head= idx(rand.nextInt(len))
      val tmp = idx.filterNot(x => x == head)
      head +: this.takeSample(tmp, n-1, rand)
    }
  }

  private def randSample(idx: Array[String], n: Int, x: Int): (Set[String], Set[String]) = {
    this.rng.setSeed(rng.nextInt(x + 42))
    val sample = this.takeSample(idx, n, rng).toSet
    val remain = idx.toSet.diff(sample)
    (sample, remain)
  }

  private def trainValidSplit(w: Double): Array[String] => Array[(Set[String], Set[String])] = {
    idx => {
      val len = idx.length
      val trainLen = (len * w).toInt
      new Array[Int](this.fold).map(x => this.randSample(idx, trainLen, x))
    }
  }

  private def factorInit(keys: Array[Int]): Array[(Int, Vector)] = {
    val rank = this.d
    keys.map(key => {
      val tmp = new Array[Double](rank).map(_ => this.rng.nextDouble())
      val value = new Vector(tmp).unitVector()
      (key, value)
    })
  }

  private def max(a: Int, b: Int): Int = if (a > b) a else b
  private def min(a: Double, b: Double): Double = if (a > b) b else a

  private def getPerform(fu: Map[Int, Vector], fv: Map[Int, Vector]): Value => Double = {
    p => {
      val ui = fu(p.i)
      val vj = fv(p.j)
      val ni = p.ni
      val N  = max(ni.length, 1)
      val p1 = ni.foldLeft(ui.dot(vj) * p.m0)((agg, q) => {
        val l = q._1
        val m0 = q._2
        agg + fu(l).dot(vj) * m0 / N
      })
      (p1 - p.m1) * (p1 - p.m1)
    }
  }

  private def nextIdx(last: Int): Int = {
    var tmp = this.rng.nextInt(this.d)
    while (tmp == last){
      tmp = this.rng.nextInt(this.d)
    }
    tmp
  }

  private def newtownV(idx: Int, fu: Map[Int, Vector], fv: Map[Int, Vector]): Value => (Int, Param) = {
    p => {
      val ui = fu(p.i)
      val vj = fv(p.j)
      val ni = p.ni
      val N  = max(ni.length, 1)
      val gamma = ni.foldLeft(ui.dot(vj) * p.m0)((agg, l) => {
        val ul = fu(l._1)
        val m0 = l._2
        agg + ul.dot(vj) * m0 / N
      })
      val alpha = ni.foldLeft(ui(idx) * p.m0)((agg, l) => {
        val ul = fu(l._1)
        val m0 = l._2
        agg + ul(idx) * m0 / N
      })
      (p.j, Param(alpha * alpha, alpha * gamma))
    }
  }

  private def newtownU(idx: Int, fu: Map[Int, Vector], fv: Map[Int, Vector]): Value => Seq[(Int, Param)] = {
    p => {
      val ui = fu(p.i)
      val vj = fv(p.j)
      val ni = p.ni
      val N  = max(ni.length, 1)
      val alpha = vj(idx) * p.m0
      val gamma = ni.foldLeft(ui.dot(vj) * p.m0)((agg, l) => {
        val ul = fu(l._1)
        val m0 = l._2
        agg + ul.dot(vj) * m0 / N
      })
      val val1 = Param(alpha * alpha, alpha * gamma)
      val val2 = ni.map(l => {
        val m0 = l._2
        val beta = m0 * vj(idx) / N
        (l._1, Param(beta * beta, beta * gamma))
      })
      (p.i, val1) +: val2
    }
  }

  private def learnGradientU(idx: Int, fu: Map[Int, Vector], fv: Map[Int, Vector]): Value => Seq[(Int, Double)] = {
    p => {
      val ui = fu(p.i)
      val vj = fv(p.j)
      val ni = p.ni
      val N  = max(ni.length, 1)
      val p1 = ni.foldLeft(ui.dot(vj) * p.m0)((agg, l) => {
        val ul = fu(l._1)
        val m0 = l._2
        agg + ul.dot(vj) * m0 / N
      })
      val w1 = p1 - p.m1
      val vjd= vj(idx)
      (p.i, w1 * vjd * p.m0) +: ni.map(l => (l._1, w1 * vjd * l._2 / N))
    }
  }

  private def learnGradientV(idx: Int, fu: Map[Int, Vector], fv: Map[Int, Vector]): Value => (Int, Double) = {
    p => {
      val ui = fu(p.i)
      val vj = fv(p.j)
      val ni = p.ni
      val N  = max(ni.length, 1)
      val p1 = ni.foldLeft(ui.dot(vj) * p.m0)((agg, l) => {
        val ul = fu(l._1)
        val m0 = l._2
        agg + ul.dot(vj) * m0 / N
      })
      val q1 = ni.foldLeft(ui(idx) * p.m0)((agg, l) => {
        val uld = fu(l._1)(idx)
        val m0 = l._2
        agg + uld * m0 / N
      })
      val w1 = p1 - p.m1
      (p.j, w1 * q1)
    }
  }

  private def sign(x: Double): Double = {
    if (x == 0) 0.0
    else x / abs(x)
  }

  def perform(usage: RDD[Value]): Double = {
    val sc = usage.context

    val users = usage.map(_.i).distinct().collect()
    val pkgs = usage.map(_.j).distinct().collect()
    val days = usage.map(_.t).distinct().collect()
    println("[INFO] %d users, %d pkgs, and %d days".format(users.length, pkgs.length, days.length))

    this.rng.setSeed(this.rng.nextInt(42))
    val initFactorU = this.factorInit(users)
    val initFactorV = this.factorInit(pkgs)

    val userNum = users.length
    val pkgNum = pkgs.length
    val lu = 0.0001 * sqrt(userNum)
    val lv = 0.0001 * sqrt(pkgNum)

    val parts = this.trainValidSplit(0.8)(days)

    val database = parts.map{case (trainDays, validDays) => {
      val s1BD = sc.broadcast(trainDays)
      val s2BD = sc.broadcast(validDays)
      val trainRDD = usage.filter(p => s1BD.value.contains(p.t))
      val validRDD = usage.filter(p => s2BD.value.contains(p.t))
      (trainRDD, validRDD)
    }}

    val scores = database.map{case (trainRDD, validRDD) =>
      // initial with same factor
      val fu = initFactorU.map(p => (p._1, new Vector(p._2.getValue())))
      val fv = initFactorV.map(p => (p._1, new Vector(p._2.getValue())))
      // store the best one in trian RDD
      var bestU = fu.map(p => (p._1, new Vector(p._2.getValue())))
      var bestV = fv.map(p => (p._1, new Vector(p._2.getValue())))
      var bestE = 100000.0
      // initial study-rate
      var rate = this.eta
      // epoch start
      var epoch = 0
      var flag = true
      var lastIdx = -1
      val trainSize = trainRDD.count()
      val validSize = validRDD.count()
      println("[INFO] %d for train RDD and %d for valid RDD".format(trainSize, validSize))
      while(epoch <= 20 && flag){
        var uBD = sc.broadcast(fu.toMap)
        var vBD = sc.broadcast(fv.toMap)
        val trainErr0 = sqrt(trainRDD.map(this.getPerform(uBD.value, vBD.value)).mean())
        val validErr0 = sqrt(validRDD.map(this.getPerform(uBD.value, vBD.value)).mean())
        println("[INFO] %d epoch with RMSE %f for train and %f for valid".format(epoch, trainErr0, validErr0))
        if (validErr0 < bestE){
          bestU = fu.map(p => (p._1, new Vector(p._2.getValue())))
          bestV = fv.map(p => (p._1, new Vector(p._2.getValue())))
          bestE = validErr0
        }else if (validErr0 >= 1.01 * bestE){
          flag = false
        }
        val idx = this.nextIdx(lastIdx)
        lastIdx = idx
        // update by users
        val gu = trainRDD.flatMap(this.newtownU(idx, uBD.value, vBD.value))
                         .reduceByKey(_ add _).collect().toMap
        for (ind <- 0 until userNum){
          val i = fu(ind)._1
          val ui = fu(ind)._2
          if (gu.contains(i)){
            val a2 = gu(i).alpha
            val ar = gu(i).beta
            if (ar == 0) (i, ui)
            else{
              val offset = lu * this.sign(ui(idx))// * trainSize
              val temp = ui.getValue()
              temp(idx) = temp(idx) - (ar + offset) / a2
              (i, new Vector(temp))
            }
          }else{
            (i, ui)
          }
        }
        uBD = sc.broadcast(fu.toMap)
        // val trainErr1 = sqrt(trainRDD.map(this.getPerform(uBD.value, vBD.value)).mean())
        // val validErr1 = sqrt(validRDD.map(this.getPerform(uBD.value, vBD.value)).mean())
        // println("[INFO] after pcd on users rmse %f for train and %f for valid".format(trainErr1, validErr1))
        // update by pkgs
        val gv = trainRDD.map(this.newtownV(idx, uBD.value, vBD.value))
                         .reduceByKey(_ add _).collect().toMap
        for (ind <- 0 until pkgNum){
          val j = fv(ind)._1
          val vj = fv(ind)._2
          if (gv.contains(j)){
            val a2 = gv(j).alpha
            val ar = gv(j).beta
            if (ar == 0) (j, vj)
            else{
              val offset = lv * this.sign(vj(idx))// * trainSize
              val temp = vj.getValue()
              temp(idx) = temp(idx) - (ar + offset) / a2
              (j, new Vector(temp))
            }
          }else{
            (j, vj)
          }
        }
        // val gu = trainRDD.flatMap(this.learnGradientU(idx, uBD.value, vBD.value, gBD.value, sBD.value))
        //                  .reduceByKey(_ + _).collect().toMap
        // for (ind <- 0 until userNum){
        //   val i = fu(ind)._1
        //   val ui = fu(ind)._2
        //   if (gu.contains(i)){
        //     val guik = gu(i) / trainSize
        //     val temp = ui.getValue()
        //     temp(idx) = temp(idx) - rate * (guik + lu * sign(temp(idx)))
        //     fu(ind) = (i, new Vector(temp))
        //   }else fu(ind) = (i, ui)
        // }
        // uBD = sc.broadcast(fu.toMap)
        // val trainErr1 = sqrt(trainRDD.map(this.getPerform(uBD.value, vBD.value, gBD.value, sBD.value)).mean())
        // val validErr1 = sqrt(validRDD.map(this.getPerform(uBD.value, vBD.value, gBD.value, sBD.value)).mean())
        // println("[INFO] after pcd on users rmse %f for train and %f for valid".format(trainErr1, validErr1))
        // val gv = trainRDD.map(this.learnGradientV(idx, uBD.value, vBD.value, gBD.value, sBD.value))
        //                  .collect().toMap
        // for (ind <- 0 until pkgNum){
        //   val j = fv(ind)._1
        //   val vj = fv(ind)._2
        //   if (gv.contains(j)){
        //     val gvjk = gv(j) / trainSize
        //     val temp = vj.getValue()
        //     temp(idx) = temp(idx) - rate * (gvjk + lv * sign(temp(idx)))
        //     fv(ind) = (j, new Vector(temp))
        //   }else fv(ind) = (j, vj)
        // }
        vBD = sc.broadcast(fv.toMap)
        // val trainErr2 = sqrt(trainRDD.map(this.getPerform(uBD.value, vBD.value)).mean())
        // val validErr2 = sqrt(validRDD.map(this.getPerform(uBD.value, vBD.value)).mean())
        // println("[INFO] after pcd on pkgs rmse %f for train and %f for valid".format(trainErr2, validErr2))
        // rate /= 1.0001
        epoch += 1
      }
      val uBD = sc.broadcast(bestU.toMap)
      val vBD = sc.broadcast(bestV.toMap)
      val validErr = sqrt(validRDD.map(this.getPerform(uBD.value, vBD.value)).mean())
      validErr
    }
    scores.reduce(min)
  }
}

object MyCrossValidation {

  private def nextDay(today: String): String = {
    today match {
      case "20161118" => "20161120"
      case "20170104" => "20170106"
      case "20161030" => "20171101"
      case "20161130" => "20171201"
      case _          => (today.toInt + 1).toString
    }
  }

  private def loadUsers(sc: SparkContext, sec: Int): Map[String, Int] = {
    val indexPath = "/appSpreadv2/tmp/msisdn_to_index_%d".format(sec)
    if (!isFileExist(sc, indexPath)){
      val name = "/appSpreadv2/max_components_%d".format(sec)
      sc.textFile(name).zipWithIndex().map(p => "%s,%d".format(p._1, p._2))
        .repartition(8).saveAsTextFile(indexPath)
    }
    sc.textFile(indexPath).map(_.split(',')).map(ps => (ps(0), ps(1).toInt)).collect().toMap
  }

  private def expansion(users: Set[Int], state: Set[String], g: Map[Int, Array[Int]]): Array[String] => Seq[Value] = {
    ps => {
      val i = ps(0).toInt
      val j = ps(2).toInt
      val t0= ps(1)
      val terminals = Set("20161103", "20161120", "20161202", "20161212", "20170117", "20170124", "20170205", "20170215")
      if (terminals.contains(t0)) Seq[Value]()
      else{
        val t1= nextDay(t0)
        val key = Array(i, j, t1).mkString("#")
        val m0 = 1
        val m1 = if (state.contains(key)) 1 else 0
        val tmp= g.getOrElse(i, Array[Int]()).filter(users.contains)
        val ni = tmp.map(l => {
          val key = "%d,%d,%s".format(l, j, t0)
          val ml0 = if (state.contains(key)) 1 else 0
          (l, ml0)
        })
        Seq(Value(i, j, t0, m0, m1, ni))
      }
    }
  }

  def loadGraph(sc: SparkContext, name: String, uToIndex: Map[String, Int]): Map[Int, Array[Int]] = {
    val dictBD = sc.broadcast(uToIndex)
    sc.textFile(name).map(_.split(','))
      .map(_.map(dictBD.value.apply))
      .map(ps => (ps(0), Set(ps(1))))
      .reduceByKey(_ union _)
      .map{case (src, dsts) => (src, dsts.toArray)}
      .collect().toMap
  }

  def loadUsage(sc: SparkContext, sec: Int, correspond: Map[String, Int], gname: String): RDD[Value] = {

    // graph loading
    val graph = loadGraph(sc, gname, correspond)
    val gShort = gname.split('/').last
    // define result path
    val trainPath = "/appSpreadv2/warehouse/train_%d_%s".format(sec, gShort)
    val testPath = "/appSpreadv2/warehouse/test_%d_%s".format(sec, gShort)
    println("[INFO] load usage from %s and %s".format(trainPath, testPath))
    // generate if not exist
    if (!(isFileExist(sc, trainPath) && isFileExist(sc, testPath))){
      // graphBD
      val gBD = sc.broadcast(graph)
      // string2index
      val refBD = sc.broadcast(correspond)
      // load usage
      val filename = "/appSpreadv2/appUsage"
      // correspond users to index
      val whole = sc.textFile(filename).map(_.split('|'))
      println("[INFO] there are %d records in whole usage".format(whole.count()))
      // filter by users
      val valid0 = whole.filter(ps => refBD.value.contains(ps(0)))
                        .map(ps => refBD.value(ps(0)).toString() +: ps.tail)
      // filter by pkg more than 10 users
      val pkgCnt = valid0.map(ps => (ps(2), Set(ps(0)))).reduceByKey(_ union _)
      val pkgGe10 = pkgCnt.filter(_._2.size >= 10).map(_._1).collect().toSet
      val validPkg = sc.broadcast(pkgGe10)
      val valid1 = valid0.filter(ps => validPkg.value.contains(ps(2)))
      println("[INFO] there are %d records remains".format(valid1.count()))
      // remains state i#j#t0
      val state = valid1.map(ps => ps.take(3).mkString("#")).distinct().collect().toSet
      val sBD = sc.broadcast(state)
      // divide data into train and test part
      val trainUsage = valid1.filter(ps => ps(1) <= "20170124")
      val testUsage  = valid1.filter(ps => ps(1) >= "20170201")
      // format usage as (i, j, t0, m0, m1, Array((l, ml0)))
      val terminals = sc.broadcast(Set("20161103", "20161120", "20161202", "20161212", "20170117", "20170124", "20170205", "20170215"))
      val trainUsers = trainUsage.filter(ps => !terminals.value.contains(ps(1)))
                                 .map(_.apply(0).toInt).collect().toSet
      println("[INFO] %d users in train part".format(trainUsers.size))
      val uInTrain = sc.broadcast(trainUsers)
      trainUsage.flatMap(expansion(uInTrain.value, sBD.value, gBD.value))
                .map(_.toString()).repartition(16).saveAsTextFile(trainPath)

      val testUsers = testUsage.filter(ps => !terminals.value.contains(ps(1)))
                               .map(_.apply(0).toInt).collect().toSet
      println("[INFO] %d users in test part".format(testUsers.size))
      val uInTest = sc.broadcast(testUsers)
      testUsage.flatMap(expansion(uInTest.value, sBD.value, gBD.value))
               .map(_.toString()).repartition(16).saveAsTextFile(testPath)
    }

    sc.textFile(trainPath).map(_.split(','))
      .map(ps => {
        val i = ps(0).toInt
        val j = ps(1).toInt
        val t0 = ps(2)
        val m0 = ps(3).toInt
        val m1 = ps(4).toInt
        val ni = if (ps.length < 6) Array[(Int, Int)]() else ps(5).split('|').map(x => x.split('#')).map(ps => (ps(0).toInt, ps(1).toInt))
        Value(i, j, t0, m0, m1, ni)
      })
  }

  def isFileExist(sc: SparkContext, filename: String): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(filename))
    exists
  }

  def generateIfNotExist(sc: SparkContext, name: String): Unit = {
    if (!isFileExist(sc, name)){
      val knn = 10
      val sec = 15
      val callGraph = sc.textFile("/appSpreadv2/graph/graph_based_logs_%d".format(sec))
                        .map(_.split(',')).map(ps => (ps(0), Set(ps(1))))
                        .reduceByKey(_ union _)
      val appGraph = sc.textFile("/appSpreadv2/graph/improved_graph_%dNN_%dsec".format(knn, sec))
                       .map(_.split(',')).map(ps => (ps(0), Set(ps(1))))
                       .reduceByKey(_ union _)
      val combGraph = callGraph.union(appGraph).reduceByKey(_ union _)
                               .flatMap{case (src, dsts) => dsts.map(dst => Array(src, dst))}
      combGraph.map(_.mkString(",")).repartition(16).saveAsTextFile(name)
    }
  }

  def apply(sc: SparkContext, fold: Int, eta: Double = 0.5): Unit ={
    Array(15, 20, 25, 30).foreach(sec => {
      println("[INFO] cross validation on max cc based on usage more than %d sec".format(sec))
      val uDict = loadUsers(sc, sec)
      val rng = new Random
      rng.setSeed(sec + 42)

      val model = new MyCrossValidation(fold, rng, 7, eta)
      println("[INFO] cross validation on direct graph more than %d sec".format(sec))
      for (knn <- 1 to 15){
        val gname1 = "/appSpreadv2/graph/direct_graph_%dNN_%dsec".format(knn, sec)
        val gname2 = "/appSpreadv2/graph/improved_graph_%dNN_%dsec".format(knn, sec)

        val usage1 = loadUsage(sc, sec, uDict, gname1)
        val usage2 = loadUsage(sc, sec, uDict, gname2)

        val perform1 = model.perform(usage1)
        val perform2 = model.perform(usage2)

        println("[INFO] for %dNN %d-fold cross-validation with rank 7 mean-RMSE direct %f and improved %f".format(knn, fold, perform1, perform2))
      }

      val gname = "/appSpreadv2/graph/combined_graph"
      generateIfNotExist(sc, gname)
      val combUsage = loadUsage(sc, sec, uDict, gname)
      for (d <- 1 to 15){
        val model1 = new MyCrossValidation(fold, rng, d, eta)
        val perform = model1.perform(combUsage)
        println("[INFO] for 10nn %d-fold corss-validation with rank %d mean-RMSE in combined network %f".format(fold, d, perform))
      }
    })
  }
}
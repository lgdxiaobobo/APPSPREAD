package jyb.zju

import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.math.{log, sqrt, exp}

case class LFM(d: Int) {
  private def logLoss: (Double, Double) => Double = {
    (r, p) => {
      val y = 1.0 / (1.0 + exp(-p))
      -r * log(y) - (1 - r) * log(1 - y)
    }
  }

  private def makeup(i: String, pos: Set[String], all: Array[String], rng: Random): (String, Set[String], Set[String]) = {
    val size = pos.size
    rng.setSeed(rng.nextInt() + 42 + size)
    val unused = rng.shuffle(all.filter(!pos.contains(_)).toSeq)
    val neg = unused.take(size).toSet
    (i, pos, neg)
  }

  private def expansion(i: String, pos: Set[String], neg: Set[String]): Seq[(String, String, Double)] = {
    val posExpand = pos.toSeq.map(j => (i, j, 1.0))
    val negExpand = neg.toSeq.map(j => (i, j, 0.0))
    posExpand ++ negExpand
  }

  private def logistic(d: Double): Double = {
    1.0 / (1.0 + exp(-d))
  }

  private def gradientU(ui: Vector, vj: Vector, rij: Double): Vector = {
    val pij = logistic(ui.dot(vj))
    vj.mul(pij - rij)
  }

  private def gradientV(ui: Vector, vj: Vector, rij: Double): Vector = {
    val pij = logistic(ui.dot(vj))
    ui.mul(pij - rij)
  }

  def latentFactor(status: RDD[(String, Set[String])], study_rate: Double): Map[String, Vector] = {
    var eta = study_rate
    val sc = status.context
    val users = status.map(_._1).collect()
    val pkgs = status.map(_._2).reduce(_ union _).toArray
    val M = users.length
    val N = pkgs.length
    println("[INFO] there are %d users and %d apps".format(M, N))
    val rng = new Random
    rng.setSeed(M + 42)
    val userFactor = users.map(i => (i, new Vector(d, rng)))
    rng.setSeed(N + 42)
    val pkgFactor = pkgs.map(j => (j, new Vector(d, rng)))
    // 5-fold cross validation
    val pkgsBD = sc.broadcast(pkgs)
    var bestFu = Array[(String, Vector)]()
    var minErr = 1000000.0
    val lu = 0.0001 * sqrt(M)
    val lv = 0.0001 * sqrt(N)
    for (cv <- 0 to 5){
      rng.setSeed(rng.nextInt() + 42 * cv)
      val trainRDD = status.map{case (i, used) => this.makeup(i, used, pkgsBD.value, rng)}
        .flatMap{case (i, pos, neg) => expansion(i, pos, neg)}
      // val size = trainRDD.count()
      val fu = userFactor.clone()
      val fv = pkgFactor.clone()
      var nowU = fu.clone()
      var nowV = fv.clone()
      var col  = 100000.0
      var flag = true
      var step = 0
      while (flag && step < 800){
        var fuBD = sc.broadcast(fu.toMap)
        val fvBD = sc.broadcast(fv.toMap)

        val trainErr = trainRDD.map{case (i, j, rij) => logLoss(rij, fuBD.value(i).dot(fvBD.value(j)))}.mean()
        val statusErr = status.flatMap{case (i, used) => used.map(j => logLoss(v1 = 1.0, fuBD.value(i).dot(fvBD.value(j))))}.mean()
        if (step % 10 == 0)
          println("[INFO] at step-%d stage with log-loss %f in train and %f in observed".format(step / 10, trainErr, statusErr))

        if (statusErr < col) {
          nowU = fu.clone()
          nowV = fv.clone()
          col = statusErr
        }else if (statusErr > 1.001 * col){
          flag = false
        }

        val gu = trainRDD.map{case (i, j, rij) => (i, gradientU(fuBD.value(i), fvBD.value(j), rij))}
          .reduceByKey(_ plus _).collect().toMap
        for (idx <- 0 until M){
          val p = fu(idx)
          val i = p._1
          val ui= p._2
          val delta = gu(i).weightedPlus(ui, w2 = lu)
          fu(idx) = (i, ui.weightedPlus(delta, w2 = -eta))
        }
        fuBD = sc.broadcast(fu.toMap)

        val gv = trainRDD.map{case (i, j, rij) => (j, gradientV(fuBD.value(i), fvBD.value(j), rij))}
          .reduceByKey(_ plus _).collect().toMap
        for (idx <- 0 until N){
          val p = fv(idx)
          val j = p._1
          val vj= p._2
          val delta = gv(j).weightedPlus(vj, w2 = lv)
          fv(idx) = (j, vj.weightedPlus(delta, w2 = -eta))
        }
        step += 1
        eta /= 1.0001
      }
      val fuBD = sc.broadcast(nowU.toMap)
      val fvBD = sc.broadcast(nowV.toMap)

      val err = status.flatMap{case (i, used) => used.map(j => logLoss(1.0, fuBD.value(i).dot(fvBD.value(j))))}.mean()
      println("[INFO] at %d-cv with %f log-loss".format(cv, err))
      if (err < minErr){
        minErr = err
        bestFu = nowU.clone()
      }
    }
    bestFu.toMap
  }
}

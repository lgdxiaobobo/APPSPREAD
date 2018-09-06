package jyb.zju

import java.io.Serializable

import scala.math.{abs, sqrt}
import scala.util.Random

class Vector(n: Int) extends Serializable {

  private val value = new Array[Double](n)

  private def randomValue(rng: Random): Unit = {
    for (idx <- 0 until n){
      this.value(idx) = abs(rng.nextDouble())
    }
    val scale = this.norm()
    for (idx <- 0 until n) {
      this.value(idx)  = this.value(idx) / scale
    }
  }

  def this(given: Array[Double]) = {
    this(given.size)
    this.setValue(given)
  }

  def this(n: Int, rng: Random) = {
    this(n)
    this.randomValue(rng)
  }

  def setValue(given: Array[Double]): Unit = {
    for (idx <- 0 until n){
      this.value(idx) = given(idx)
    }
  }

  def apply(idx: Int): Double = this.value(idx)

  def getValue(): Array[Double] = this.value

  def norm(): Double = {
    val ss = this.value.map(x => x * 2).sum
    sqrt(ss)
  }

  def dot(other: Vector): Double = {
    val mix = this.value.zip(other.getValue())
    mix.map{case (a, b) => a * b}.sum
  }

  def plus(other: Vector): Vector = {
    val mix = this.value.zip(other.value)
    val given = mix.map{case (a, b) => a + b}
    new Vector(given)
  }

  def mul(lambda: Double): Vector = {
    val given = this.value.map(x => x * lambda)
    new Vector(given)
  }

  def weightedPlus(other: Vector, w1: Double, w2: Double): Vector = {
    val given = this.value.zip(other.value).map{case (a, b) => a * w1 + b * w2}
    new Vector(given)
  }

  def weightedPlus(other: Vector, w2: Double): Vector = {
    weightedPlus(other, 1.0, w2)
  }

  def minus(other: Vector): Vector = {
    weightedPlus(other, 1.0, -1.0)
  }

  def unitVector(): Vector = {
    val scale = this.norm()
    val unitVal = this.value.map(x => abs(x) * 1.0 / scale)
    new Vector(unitVal)
  }

}

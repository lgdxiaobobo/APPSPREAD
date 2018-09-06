package jyb.zju

import java.io.Serializable

class TopNList(N: Int) extends Serializable {

  var topN = Array[Node]()

  def add(e: Node): Unit = {
    if (topN.size == this.N) addInHeap(e)
    else addInLst(e)
  }

  private def addInHeap(e: Node): Unit = {
    if (this.topN.head.smaller(e)){
      this.topN = fitHeap(e +: this.topN.tail)
    }
  }

  private def fitHeap(lst: Array[Node]): Array[Node] = {
    var curr = 0
    val size = lst.size
    val x = lst.head
    var flag = true
    while (curr < size && flag){
      val left = (curr << 1) + 1
      val right= left + 1
      flag = false
      if (right < size && lst(right).smaller(lst(curr))){
        lst(curr) = lst(right)
        flag = true
        curr = right
      }else if (left < size && lst(left).smaller(lst(curr))){
        lst(curr) = lst(left)
        flag = true
        curr = left
      }
    }
    lst(curr) = x
    lst
  }

  private def addInLst(e: Node): Unit = {
    this.topN = this.topN :+ e
  }

  private def toHeap(lst: Array[Node]): Array[Node] = {
    val half = this.N >> 1
    val temp = lst.clone()
    for (idx <- half to 1 by -1){
      var curr = idx - 1
      val x = temp(curr)
      var flag = true
      while (curr < this.N && flag){
        val left = (curr << 1) + 1
        val right = left + 1
        flag = false
        if (right < this.N){
          if (temp(right).smaller(temp(curr))){
            temp(curr) = temp(right)
            curr = right
            flag = true
          }
        }else{
          if (temp(left).smaller(temp(curr))){
            temp(curr) = temp(left)
            curr = left
            flag = true
          }
        }
      }
      lst(curr) = x
    }
    temp
  }
}

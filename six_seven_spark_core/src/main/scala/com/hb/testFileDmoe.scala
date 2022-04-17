package com.hb

import scala.collection.mutable

object testFileDmoe {


  def main(args: Array[String]): Unit = {
    val dui = "\"good morning\""

    val str = dui.stripPrefix("\"").stripSuffix("\"")
//    str.stripSuffix("\"")
    println(str)
    println(dui)

    val maps = mutable.HashMap[String, Int]()

    val m1 = mutable.HashMap("a"->1)
    val m2 = mutable.HashMap("b"->2)
//    val m4 = mutable.HashMap("b",2)
    val m3 = m1++m2
    println(m3.toString())

    val myStrArr = new mutable.ArrayBuffer[(Int,Int)]

    val m6 = mutable.HashMap(2 -> 1, 1 -> 1,5 -> 3, 0 -> 2)
    val ints = m6.keySet.toArray.sorted
    var result_tp = Array()
    for (ii <- ints){
      println(ii+"\t"+m6(ii))
      myStrArr.append((ii,m6(ii)))
    }

    myStrArr.foreach(x=>println(x._1,x._2))



    ints.foreach(println(_))
//    print(ints.toString)

  }

}

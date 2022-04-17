package com.hb

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.random

object invertedIndex {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("invertedIndex")
    val sc = new SparkContext(conf)

    val project_BASEPATH = "/Users/zhanghuaibei/Documents/java_scala_spark_flink/spark_practice/geek_BigData_Train_temp/six_seven_spark_core"
    val  data_path=project_BASEPATH+"/data.txt"
//    val  data_path=project_BASEPATH+"/data_simple.txt"
    val inputrdd = sc.textFile(data_path)
//    inputrdd.foreach(println(_))

    val inverted_index = inputrdd.map(line => {
      val strArr = line.split("\\.")
//      println(line)
//      println("strArr len:"+strArr.length+"\t"+strArr(0))
      (strArr(0),strArr(1))
    })
      //将数据进行flatMap数据分割并将文件的index放到数据里面
      .flatMap(line=>{
        val index = line._1
        val content = line._2
        val content_Strip = content.stripPrefix("\"").stripSuffix("\"")
        val content_Split = content_Strip.split(" ")
        content_Split.map(word => {
          ((word,index),1)
        })
      })
      //将相同单词和文件的数据加起来
      .reduceByKey(_+_)
      //再将数据拆分成以单词为key的以index和次数为map的格式，这样做是为了reduceByKey时方便
      .map(tup=>(tup._1._1,mutable.HashMap(tup._1._2->tup._2)))
//      .map(tup=>(tup._1._1,tup._2))
      .reduceByKey(_++_)
      //上述操作之后变成这样的形式(banana,Map(2 -> 1)) 、(is,Map(2 -> 1, 1 -> 1, 0 -> 2))
      .map(word_map =>{
        val myStrArr = new mutable.ArrayBuffer[(String,Int)]

        val word = word_map._1
        val index_time = word_map._2
        val index_sort = index_time.keySet.toArray.sorted
        for (idx <- index_sort){
          myStrArr.append((idx,index_time(idx)))
        }
        (word,myStrArr)
      })
//      最终结果如下：(is,ArrayBuffer((0,2), (1,1), (2,1)))、、(banana,ArrayBuffer((2,1)))
//
//    val m6 = mutable.HashMap(2 -> 1, 1 -> 1,5 -> 3, 0 -> 2)
//    val ints = m6.keySet.toArray.sorted
//
//    for (ii <- ints){
//      println(ii+"\t"+m6(ii))
//    }

    inverted_index.foreach(println(_))






    //word count的例子
//    val resultRDD = inputrdd.filter(line => null != line && line.trim.length > 0)
//      .flatMap(_.split(" "))
//      .map(word => (word ,1))
//      .reduceByKey(_ + _)
//    println("----")
//    resultRDD.foreach(println(_))

//    resultRDD.coalesce(1)
//      .foreachPartition(
//        iter=>iter.foreach(println)
//      )



    println("This in inverted index.")


    sc.stop()

  }


}

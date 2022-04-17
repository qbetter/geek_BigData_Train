package com.hb

import org.apache.spark.sql.SparkSession

import scala.math.random

object sparkPiTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()

    //    val conf = new SparkConf() setMaster ("local[2]").setAppName("invertedIndex")
    //    val conf = new SparkConf() setMaster ("local[2]")

    //    val textFile = sc.textFile(”hdfs://data.txt")


    println("This in inverted index.")


  }


}

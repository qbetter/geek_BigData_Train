package com.hb

import org.apache.hadoop.fs.{FileUtil, Path}
//import java.io.File
import java.nio.file.FileSystem

import javax.security.auth.login.{AppConfigurationEntry, Configuration}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source
import scala.math.random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, LocatedFileStatus, Path}
import org.apache.log4j.Logger

case class SparkDictCpOptions(source: String,
                              target: String,
                              maxConcurrence: Int,
                              ignoreFailures: Boolean,
                              appName: String,
                              master: String)

case class CopyInfo(sourceFile: String,
                    targetFile: String)


object DispcpDemo {


  def main(args: Array[String]): Unit = {


    println("ppp")


  }

//  def mkdirFunc(sparkSession: SparkSession,sourcePath: Path, targetPath: Path,
//                fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit ={
//    val fs = FileSystem
////      .get(sparkSession.sparkContext.hadoopConfiguration)
//    fs.listStatus(sourcePath)
//
//
//  }


//  def copyFunc(sparkSession: SparkSession,sourcePath: Path, targetPath: Path,
//                fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit ={
//
//
//
//    val conf = new SparkConf().setMaster("local[2]").setAppName("invertedIndex")
////    val sc = new SparkContext(conf)
//    val sc = sparkSession.sparkContext
//    val maxConcurrenceTask = Some(options.maxConcurrenceTask).getOrElse(5)
//    val rdd = sc.makeRDD(fileList, maxConcurrenceTask)
//
//    rdd.mapPartitions(ite =>{
//      val hadoopConf = new Configuration()
//      ite.foreach(tup => {
//        try{
//
//
//        }catch {
//          case ex:Exception => if(!options.ignoreFailures) throw ex else println(ex.getMessage)
//        }
//      })
//      ite
//    })








//  }





}



package com.hb

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, LocatedFileStatus, Path}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI
import scala.collection.mutable.ArrayBuffer

case class SparkDictCpOptions(source: String,
                              target: String,
                              maxConcurrence: Int,
                              ignoreFailures: Boolean,
                              appName: String,
                              master: String)

case class CopyInfo(sourceFile: String,
                    targetFile: String)

object SparkDistCpDemo {

  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    val distCpOptions = parseOpts(args)

    val conf = new SparkConf()
      .setAppName(distCpOptions.appName)
      .setMaster(distCpOptions.master)
    val sc = new SparkContext(conf)

    val copyList: ArrayBuffer[CopyInfo] = getCopyFileList(distCpOptions)
    run(distCpOptions, sc, copyList)
  }

  def run(opts: SparkDictCpOptions, spark: SparkContext,
          copyList: ArrayBuffer[CopyInfo]): Unit = {
    val fileRDD: RDD[CopyInfo] = spark.parallelize(copyList, opts.maxConcurrence)

    fileRDD.foreachPartition(iter => {
      val conf = new Configuration()
      iter.foreach(copyInfo => {
        try {
          doCopy(copyInfo, conf)
        } catch {
          case e: Exception => {
            if (opts.ignoreFailures) {
              log.info("file: {} failed.", copyInfo.sourceFile, e)
            } else {
              throw e
            }
          }
        }
      })
    })
  }

  def doCopy(copyInfo: CopyInfo, conf: Configuration): Unit = {
    val sourceFile = copyInfo.sourceFile
    val targetFile = copyInfo.targetFile
    val sourceFs = FileSystem.get(new URI(toLegalUri(sourceFile)), conf)
    val targetFs = FileSystem.get(new URI(toLegalUri(targetFile)), conf)
    FileUtil.copy(sourceFs, new Path(sourceFile),
      targetFs, new Path(targetFile),
      false, conf)
  }

  def getCopyFileList(distCpOptions: SparkDictCpOptions): ArrayBuffer[CopyInfo] = {
    val copyList = new ArrayBuffer[CopyInfo]()
    val source = distCpOptions.source
    val sourcePath = new Path(source)
    val startSourcePath = sourcePath.getName

    val fs = FileSystem.get(new URI(source), new Configuration())
    val fileList = fs.listFiles(sourcePath, true)
    while (fileList.hasNext) {
      val file: LocatedFileStatus = fileList.next()
      val sourceFile: String = file.getPath.toString
      copyList += CopyInfo(sourceFile,
        sourcePathToTargetPath(startSourcePath, sourceFile, distCpOptions.target))
    }
    copyList
  }

  def sourcePathToTargetPath(startSourcePath: String,
                             sourceFile: String,
                             targetFsPath: String): String = {
    val sourcePath = new URI(toLegalUri(sourceFile))
    val filePath: String = sourcePath.getRawPath
    // println("source path: " + sourcePath + ", " + "file path: " + filePath)
    val sb = StringBuilder.newBuilder.append(targetFsPath)
    if (!targetFsPath.endsWith("/")) {
      sb.append("/")
    }
    sb.append(filePath.substring(filePath.indexOf(startSourcePath)))
    sb.toString()
  }

  def toLegalUri(input: String): String = {
    input.replaceAll(" ", "%20")
  }

  def parseOpts(args: Array[String]): SparkDictCpOptions = {
    var source = "hdfs://linux120:9000/test/"
    var target = "hdfs://linux120:9000/test2/test1"
    var maxConcurrence = 4
    var ignoreFailures = true
    var appName = "SparkDistCpDemo"
    var master = "local[*]"
    args.sliding(2, 2).toList.collect {
      case Array("--help", "") => printHelp()
      case Array("--source", value: String) => source = value
      case Array("--target", value: String) => target = value
      case Array("-i", value: String) => ignoreFailures = value.toBoolean
      case Array("-m", value: String) => maxConcurrence = value.toInt
      case Array("--appName", value: String) => appName = value
      case Array("--master", value: String) => master = value
    }
    SparkDictCpOptions(source, target, maxConcurrence, ignoreFailures, appName, master)
  }

  def printHelp(): Unit = {
    println("Usage: copy directory from source fs to target fs")
    println("       for example: from hdfs://nn1:9000/test to hdfs://nn2:9000/test1")
    println("           will be: hdfs://nn2:9000/test1/test.")
    println("--source    :", "hdfs source path to copy, required.")
    println("--target    :", "hdfs target path, required.")
    println("-i          :", "ignore failures, value: true/false, default is true.")
    println("-m          :", "max concurrency, num of executors, default is 4.")
    println("--appName   :", "spark appName, default is SparkDistCpDemo.")
    println("--master    :", "spark master, default is local[*].")
  }
}
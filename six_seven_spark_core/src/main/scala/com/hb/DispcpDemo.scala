package com.hb

import org.apache.hadoop.conf.Configuration}

import javax.security.auth.login.{AppConfigurationEntry}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source
import scala.math.random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, LocatedFileStatus, Path}
import org.apache.log4j.Logger
import java.net.URI

////代码框架是这样的,并没有经过线上验证.如果父目录不存在的话，FileUtil.copy 会自动帮你建父目录；而且listFiles可以递归地获取文件列表 可以轻松获得文件夹下包括嵌套目录的所有文件。

//使用了case class会自动创建伴生对象以及自定义一些函数
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
    val sourcePath = ""
    val targerPath = ""

    val cpinfo = CopyInfo(sourcePath, targerPath)
    val conf = new Configuration()
    this.copyFunc(cpinfo,conf)

  }

  def copyFunc(copyInfo: CopyInfo, conf: Configuration): Unit ={
    val sourceFile = copyInfo.sourceFile
    val targetFile = copyInfo.targetFile
    val sourceFs = FileSystem.get(new URI(toLegalUri(sourceFile)), conf)
    val targetFs = FileSystem.get(new URI(toLegalUri(targetFile)), conf)
    FileUtil.copy(sourceFs, new Path(sourceFile),
      targetFs, new Path(targetFile),
      false, conf)
  }

  def toLegalUri(input: String): String = {
    input.replaceAll(" ", "%20")
  }

}



package com.xyg.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
  * Author: Mr.Deng
  * Date: 2018/8/25
  * Desc:
  */
object TestDemo {
  /** Usage: HdfsTest [file] */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.appName("TestDemo").getOrCreate()
    val textFile = spark.read.textFile("TestSpark/src/in/test.txt").cache()
    val numAs = textFile.filter(line => line.contains("a")).count()
    val numBs = textFile.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}
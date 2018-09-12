package com.xyg.spark.examples

import scala.math.random
import org.apache.spark.sql.SparkSession

/**
  * Author: Mr.Deng
  * Date: 2018/8/25
  * Desc: Computes an approximation to pi
  */
object SparkPi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Pi").getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}
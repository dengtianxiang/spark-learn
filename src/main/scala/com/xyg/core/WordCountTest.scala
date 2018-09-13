package com.xyg.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Mr.Deng
  * Date: 2018/8/25
  * Desc:
  */
object WordCountTest {

  def main(args: Array[String]): Unit = {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("WordCountTest").setMaster("local")
    //创建Spark上下
    val sc = new SparkContext(conf)
    //从文件中获取数据
    val input = sc.textFile("src/in/wc.txt")
    //分析并排序输出统计结果
    input.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2,false)
      .saveAsTextFile("src/out/wc.txt")

  }
}
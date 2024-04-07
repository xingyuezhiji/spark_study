package com.tencent.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupbyExample {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("GroupbyExample").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 1), ("b", 4), ("a", 3),("c", 7)))

    rdd.groupByKey().collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}

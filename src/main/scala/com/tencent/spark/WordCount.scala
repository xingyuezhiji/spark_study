package com.tencent.spark.sparkcore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd: RDD[String] = sc.makeRDD(List("spark python", "java", "spark", "scala", "java", "spark", "hadoop", "spark"))

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    println("rdd:")
    rdd.collect().foreach(println)
    println("-------------------------------")
    println("flatRDD: ")
    flatRDD.collect().foreach(println)
    println("-------------------------------")
    println("mapRDD: ")
    mapRDD.collect().foreach(println)
    println("-------------------------------")
    println("resRDD: ")
    resRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}

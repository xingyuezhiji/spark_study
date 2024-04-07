package com.tencent.spark

import org.apache.spark.sql.SparkSession

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("flatmap_example")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val data = Seq("Project Gutenberg’s",
      "Alice’s Adventures in Wonderland",
      "Project Gutenberg’s",
      "Adventures in Wonderland",
      "Project Gutenberg’s")

    import spark.sqlContext.implicits._
    val df = data.toDF("data")
    df.show(false)

    val mapDF=df.map(x=> {
      x.getString(0).split(" ")
    })
    mapDF.show(false)

    //Flat Map Transformation
    val flatMapDF=df.flatMap(fun=>
    {
      fun.getString(0).split(" ")
    })
    flatMapDF.show()
  }

}

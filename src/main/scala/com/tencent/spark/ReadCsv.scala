package com.tencent.spark
import org.apache.spark.sql.{SparkSession, DataFrame}

object ReadCsv {
  def main(args: Array[String]): Unit = {
    def readCsvAsDataFrame(file: String): DataFrame = {
      //val spark = SparkSession.builder.getOrCreate()
      val spark: SparkSession = SparkSession.builder()
        .master("local[1]")
        .appName("readcsv")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      val df = spark.read
        .option("header", "true") // 第一行包含列名
        .option("inferSchema", "true") // 推断列的数据类型  .option("header", "true") // 第一行包含列名
        .option("nrows", 10)
        .csv(file)
      df.show()
      df
    }
    //
    println("user data:")
    val user = readCsvAsDataFrame("./data/user.csv")
    println("ad data:")
    val ad = readCsvAsDataFrame("./data/ad.csv")
    println("click data:")
    val click_log = readCsvAsDataFrame("./data/click_log.csv")


    //println("user data:")
    //val user = readCsvAsDataFrame("/Users/jusbinzhong/python_project/data_mining/data_process/data/user_info.csv")
    //println("ad data:")
    //val ad = readCsvAsDataFrame("/Users/jusbinzhong/python_project/data_mining/data_process/data/ad_info.csv")
    ////println("expo log:")
    ////val expo_log = readCsvAsDataFrame("/Users/jusbinzhong/python_project/data_mining/data_process/data/exposure_log.csv")
    //println("user_app_install:")
    //val user_app_install = readCsvAsDataFrame("/Users/jusbinzhong/python_project/data_mining/data_process/data/user_app_install.csv")
  }
}

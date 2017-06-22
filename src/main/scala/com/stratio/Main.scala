package com.stratio


import Outputs.PostgresOutput
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9,10)).toDF("numbers")

    if(args(0) == "security"){
      val rootCAPath = sc.getConf.getOption("spark.ssl.datastore.rootCaPath")
      require(rootCAPath.isDefined, "Trying to connect with SSl but not rootCAPath obtained, Exit")
      PostgresOutput(spark, args(1), args(2), args(3), args(4), rootCAPath).saveWithKeyAndTrust(df)
    }
    else {
      println("************************************ no security **********************************")
      PostgresOutput(spark, args(1), args(2), args(3), args(4), None).saveWithoutKeyAndTrust(df)
    }


  }
}

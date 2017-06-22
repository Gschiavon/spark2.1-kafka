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
      .appName("spark-psql-hdfs")
      .getOrCreate()

    import spark.implicits._

    val psqlHostPortDataBase = args(0)
    val user = args(1)
    val tableName = args(2)
    val hdfsPath = args(3)

    sys.env.foreach(println)

    val psql = PostgresOutput(spark, psqlHostPortDataBase, user, tableName, hdfsPath)

    //Read from Postgres
    val df = psql.readFromPsql
    df.show()

    //Write to HDFS
    val path = psql.saveToHDFS(df)
    spark.sparkContext.textFile(path).collect.foreach(println)
  }


}


//println(s"KEY $key, VALUE $value "))
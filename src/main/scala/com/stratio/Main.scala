package com.stratio

import org.apache.spark._
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._



object Main {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("elasticsearch-test")

    val elasticOptions = extractElasticSecurityOption(args(0), args(1), sparkConf)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    import spark.implicits._

    elasticOptions.foreach({
      case (key, value) => println(s"KEY: $key, VALUE: $value")
    })


    println("********* Writing **********")
    val dataset1 = spark.createDataset(List(1,2,3,4,5,6))
    val result = dataset1
      .write
      .format("org.elasticsearch.spark.sql")
      .options(elasticOptions)
      .mode(Append)
      .save()

    println("********* Reading **********")
    val dataset2 = spark
      .read
      .format("org.elasticsearch.spark.sql")
      .options(elasticOptions)
      .load()
    println(dataset2.rdd.getNumPartitions)
    dataset2.printSchema()
    dataset2.show(10)
  }

  def extractElasticSecurityOption(host: String, port: String, sparkConf: SparkConf): Map[String, String] = {

    val options = Map("es.index.auto.create" -> "true",
      "es.nodes"-> host,
      "es.resource" -> "test/type1",
      "es.port"-> port)

    val prefixSparkElastic = "spark.ssl.datastore."
    val prefixElasticSecurity = "es.net.ssl"

    if (sparkConf.getOption(prefixSparkElastic + "enabled").isDefined
      && sparkConf.get(prefixSparkElastic + "enabled") == "true") {

      val configElastic = sparkConf.getAllWithPrefix(prefixSparkElastic).toMap
      options ++ Map( s"$prefixElasticSecurity" -> configElastic("enabled"),
        s"$prefixElasticSecurity.keystore.pass" -> configElastic("keyStorePassword"),
        s"$prefixElasticSecurity.keystore.location" -> s"file:${configElastic("keyStore")}",
        s"$prefixElasticSecurity.truststore.location" -> s"file:${configElastic("trustStore")}",
        s"$prefixElasticSecurity.truststore.pass" -> configElastic("trustStorePassword"))
    } else {
      Map()
    }
  }
}




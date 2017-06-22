package Outputs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode._


case class PostgresOutput(spark: SparkSession, url: String, user: String, tableName: String, hdfsPath: String) {



  def writeToPsql(dataFrame: DataFrame): Unit = {

    val sslCert = s"${sys.env("SPARK_SSL_CERT_PATH")}/cert.crt"
    val sslKey = s"${sys.env("SPARK_SSL_CERT_PATH")}/key.pkcs8"
    val sslRootCert = s"${sys.env("SPARK_SSL_CERT_PATH")}/caroot.crt"

    val value = s"jdbc:postgresql://${url}?ssl=true&sslmode=verify-full&sslcert=$sslCert&sslrootcert=$sslRootCert&sslkey=$sslKey"

    dataFrame.write
      .format("jdbc")
      .option("url", value)
      .option("dbtable", "postgres")
      .option("user", "job-spark-1")
      .option("driver", "org.postgresql.Driver")
      .save()

  }

  def readFromPsql(): DataFrame = {

    val sslCert = s"${sys.env("SPARK_SSL_CERT_PATH")}/cert.crt"
    val sslKey = s"${sys.env("SPARK_SSL_CERT_PATH")}/key.pkcs8"
    val sslRootCert = s"${sys.env("SPARK_SSL_CERT_PATH")}/caroot.crt"


    val value = s"jdbc:postgresql://${url}?ssl=true&sslmode=verify-full&sslcert=$sslCert&sslrootcert=$sslRootCert&sslkey=$sslKey"

    println("############################################# PSQL CHAIN ############################################# ")
    println(value)
    println("############################################# PSQL CHAIN ############################################# ")

    spark.read
      .format("jdbc")
      .option("url", value)
      .option("dbtable", "pg_stat_activity")
      .option("user", "job-spark-1")
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def saveToHDFS(dataFrame: DataFrame): String = {
    val value = s"$hdfsPath${System.currentTimeMillis}/"
    dataFrame
      .write
      .mode(Append)
      .format("csv")
      .option("path", value)
      .save()
    value
  }
}

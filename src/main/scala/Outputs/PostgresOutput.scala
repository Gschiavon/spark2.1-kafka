package Outputs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode._


case class PostgresOutput(spark: SparkSession, url: String, tableName: String,
                          user: String, password: String, rootCAPath: Option[String] = None) {

  def saveWithKeyAndTrust(dataFrame: DataFrame): Unit = {
    val sslfactory = "org.postgresql.ssl.NonValidatingFactory"

    val value = s"jdbc:postgresql://${url}?user=postgres&ssl=true&sslmode=verify-full&sslrootcert=${rootCAPath.get}&sslfactory=$sslfactory"


    println("############################################# PSQL CHAIN ############################################# ")
    println(value)
    println("############################################# PSQL CHAIN ############################################# ")

    dataFrame.write
      .mode(Append)
      .format("jdbc")
      .option("url", value)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .save()
  }

  def saveWithoutKeyAndTrust(dataFrame: DataFrame): Unit = {
//
//    val sslCert = "/tmp/pgspark.crt"
//    val sslKey = "/tmp/pgspark.key"
//    val sslRootCert = "/tmp/caroot.crt"

    val sslCert = s"${sys.env("SPARK_SSL_CERT_PATH")}/cert.crt"
    val sslKey = s"${sys.env("SPARK_SSL_CERT_PATH")}/key.pkcs8"
    val sslRootCert = s"${sys.env("SPARK_SSL_CERT_PATH")}/caroot.crt"


    val value = s"jdbc:postgresql://${url}?ssl=true&sslmode=verify-full&sslcert=$sslCert&sslrootcert=$sslRootCert&sslkey=$sslKey"

    println("############################################# PSQL CHAIN ############################################# ")
    println(value)
    println("############################################# PSQL CHAIN ############################################# ")

    dataFrame.write
      .mode(Append)
      .format("jdbc")
      .option("url", value)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .save()
  }
}

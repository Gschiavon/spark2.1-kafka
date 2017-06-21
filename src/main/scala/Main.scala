import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-kafka")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test"
    )


    val kafkaSSLParams = extractKafkaSecurityOption(kafkaParams, conf)

    val topics = Array(args(1))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaSSLParams)
    )

    stream.map(record => (record.key, record.value))

    stream.foreachRDD(rdd =>
    println(s"*************###########*******************${rdd.count}*************##########*******************")
    )
    ssc.start()
    ssc.awaitTermination()
  }
  def extractKafkaSecurityOption(kafkaParams: Map[String, AnyRef],
                                 sparkConf: SparkConf): Map[String, AnyRef] = {
    val prefixKafka = "spark.ssl.kafka."
    if (sparkConf.getOption(prefixKafka + "enabled").isDefined && sparkConf.get(prefixKafka + "enabled") == "true") {
      val configKafka = sparkConf.getAllWithPrefix(prefixKafka).toMap
      kafkaParams ++ Map[String, String]("security.protocol" -> "SSL",
        "ssl.key.password" -> configKafka("keyPassword"),
        "ssl.keystore.location" -> configKafka("keyStore"),
        "ssl.keystore.password"-> configKafka("keyStorePassword"),
        "ssl.truststore.location"-> configKafka("trustStore"),
        "ssl.truststore.password"-> configKafka("trustStorePassword"))

    } else kafkaParams
  }
}



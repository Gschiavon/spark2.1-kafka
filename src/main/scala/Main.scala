
import org.apache.spark._

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-kafka")
    val sc = new SparkContext(conf)

    val list = (1 to 100000).toList
    val numbers = sc.parallelize(list)
    numbers.collect.foreach(println)
  }
}



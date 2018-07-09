package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext, SparkConf}
import domain._
object BatchJob {
  def main(args: Array[String]): Unit = {

    //Set the Spark Context
    val conf = new SparkConf().setAppName("Lambda with spark")

    //check if running from IDE
    if(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")){
      System.setProperty("hadoop.home.dir", "E:\\OracleVB\\WinUtils")
      conf.setMaster("local")
    }

    //new spark context

    val sc = new SparkContext(conf)

    val soureFile = "file:///E:/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
    val input = sc.textFile(soureFile)
    //input.foreach(println)

    val inputRDD = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }
      //.toDF()

//    val df = inputDF.select(
//      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_hour"),
//      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
//    ).cache()

    val keyedByProduct = inputRDD.keyBy(x=>(x.product, x.timestamp_hour)).cache()
    val visitorsByProduct = keyedByProduct
      .mapValues(x=>x.visitor)
      .distinct()
      .countByKey()

    val activityByProduct = keyedByProduct.mapValues(x=>
      x.action match {
        case "purchase" => (1,0,0)
        case "add_to_cart" => (0,1,0)
        case "page_view" => (0,0,1)
      }
    ).reduceByKey( (x,y)=>(x._1+y._1, x._2+y._2, x._3+y._3))

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}

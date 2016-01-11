package bi.megam

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{ Config, ConfigFactory }


trait SparkContextConfig {

  private val config =  ConfigFactory.load()

  private lazy val sparkConf = {
    val master = "spark://103.56.92.23:7077" //get from config - set ip of spark cluster
    new SparkConf()
          .setMaster(master)
          .setAppName("meglytics")
          .set("spark.driver.allowMultipleContexts", "true")
          .set("spark.executor.extraClassPath", "/root/spark-1.5.1/lib/mysql-connector-java-5.1.34.jar")

  }
   val sc = new SparkContext(sparkConf)

}

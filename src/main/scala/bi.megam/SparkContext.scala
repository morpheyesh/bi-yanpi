package bi.megam

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{ Config, ConfigFactory }


trait SparkContextConfig {

  private val config =  ConfigFactory.load()

  private lazy val sparkConf = {
    val master = "local[4]" //get from config - set ip of spark cluster
    new SparkConf().
          setMaster(master)
  }
  lazy val sc = new SparkContext(sparkConf)
}

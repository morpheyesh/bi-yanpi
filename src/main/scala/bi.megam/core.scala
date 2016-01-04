package bi.megam

import org.apache.spark._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.rdd.RDD
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Try

object Main extends spark.jobserver.SparkJob with SparkContextConfig {

  def main(args: Array[String]) {
    println("I'm born")
    val config = ConfigFactory.parseString("")
    val res = runJob(sc, config)
  }

  def validate(sc: SparkContext, config: Config): spark.jobserver.SparkJobValidation = spark.jobserver.SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {
    //this class gets called
    //1. query and wb id will come in - fetch from riak the wb data

    //2. figure out the number of connectors for data fetching
    //3. call respective connectors and fetch data
    //4. apply query rules
    //5. apply logic and get data



  }


}

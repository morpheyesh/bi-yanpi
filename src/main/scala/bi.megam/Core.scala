package bi.megam

import org.apache.spark._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.rdd.RDD
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Try
import org.megam.common.riak.GunnySack
import scala.collection.JavaConverters._
import org.apache.spark.sql.SQLContext


object Main extends spark.jobserver.SparkJob with SparkContextConfig {

  def main(args: Array[String]) {
    val config = ConfigFactory.load()
    val res = runJob(sc, config)
  }

  def validate(sc: SparkContext, config: Config): spark.jobserver.SparkJobValidation = spark.jobserver.SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {
    //1. query and wb id will come in - fetch from riak the wb data
      val listDF = config.getObjectList("input.json.connectors").asScala.map(_.unwrapped())
         .map(i =>
        Connectors(sc, i.get("source").toString, i.get("cred").toString, i.get("tables").toString))
        val data = Engine(config.getObject("input.json").toString, listDF.toSet)
       sc.stop()
   return listDF

  }
}

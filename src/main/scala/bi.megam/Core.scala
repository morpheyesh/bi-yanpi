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
     val query = config.getObject("input.json")
     println(sc)
      val connectors = config.getObjectList("input.json.connectors").asScala.map(_.unwrapped())
      val finalrdd = connectors.map(i => Connectors(sc, i.get("source").toString, i.get("cred").toString, i.get("tables").toString))


   return finalrdd
      // WorkBench(input) //to parse data

    //2. figure out the number of connectors for data fetching

      //  val source = WorkBench.dSource()

    //3. call respective connectors and fetch data with map of table names

       //val tables = Map("tables" -> "one")
       //val SourceData = Connectors("mysql", tables)

    //4. apply query rules - send mappings and SourceData

     //Engine(query, sdata).execute()
  }

}

object SqlJob extends SparkSqlJob {
  def validate(sql: SQLContext, config: Config): spark.jobserver.SparkJobValidation = spark.jobserver.SparkJobValid

  def runJob(sql: SQLContext, config: Config): Any = {
    println("-------------------------------------------")
    println(sql)
    println("-------------------------------------------")

    sql.sql(config.getString("sql")).collect()
  }
}

trait SparkSqlJob extends spark.jobserver.SparkJobBase {
  type C = SQLContext
}

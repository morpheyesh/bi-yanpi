package bi.megam

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import java.sql.DriverManager



trait Connectors
object Connectors extends SqlContext {
  def apply(sc: SparkContext, source: String, cred: String, tables: String): DataFrame = source match {
    case "mysql" => return new Mysql().get(sc, cred, tables)
    case "postgres" => new Postgresql().get(sc ,cred, tables)
  }
}

  private class Mysql() extends Connector {

    def get(sc: SparkContext, c: String, t: String): DataFrame = {

      val sqlContext = new SQLContext(sc)
      val url="jdbc:mysql://103.56.92.47:3306/fooDatabase"
      val prop = new java.util.Properties
      prop.setProperty("user", "fooUser")
      prop.setProperty("password", "megam")

      val pet = sqlContext.read.jdbc(url,"pet",prop)
     return pet
    }
  }
  private class Postgresql() extends Connectors {
    def get(sc: SparkContext, c: String, t: String): DataFrame = {
      val sqlContext = new SQLContext(sc)
       println(sqlContext)
    //   sc.stop()
      val url="jdbc:mysql://103.56.92.47:3306/fooDatabase"
      val prop = new java.util.Properties
      prop.setProperty("user", "fooUser")
      prop.setProperty("password", "megam")


      val pet = sqlContext.read.jdbc(url,"pet",prop)
  
     sc.stop()
     return pet

    }
  }

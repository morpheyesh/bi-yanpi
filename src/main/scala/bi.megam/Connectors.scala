package bi.megam

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager



trait Connectors
object Connectors {
  def apply(sc: SparkContext, source: String, cred: String, tables: String) = source match {
    case "mysql" => new Mysql().get(sc, cred, tables)
    case "postgres" => new Postgresql().get(sc ,cred, tables)
  }
}

  private class Mysql() extends Connectors {
//returns the data of all the tables combined
    def get(sc: SparkContext, c: String, t: String) {

      val sqlContext = new SQLContext(sc)
       println(sqlContext)
    //   sc.stop()
      val url="jdbc:mysql://103.56.92.25:3306/megamtest"
      val prop = new java.util.Properties
      prop.setProperty("user", "boo")
      prop.setProperty("password", "megam")


    //  val people = sqlContext.read.jdbc(url,"person",prop)
    val boo =  sqlContext.jdbc("jdbc:mysql://103.56.92.25:3306/megamtest?user=boo&password=megam", "person")
    

    }
  }
  private class Postgresql() extends Connectors {
    def get(sc: SparkContext, c: String, t: String) {}
  }

package bi.megam

import org.apache.spark.sql.DataFrame

case class Engine(query: String, df: List[List[DataFrame]]) extends rulePredicates {



  private def applyRules(q: String): List[String] = {
    //yet to apply rules
       return q.split(" ").toList


  }

  def execute() {
    val newQuery = applyRules(query)
    //based on newQuery, manipulate the data
    //ex: date, revenue
    //take all date data and its respective mappings
  
  println(df(0)(0).select("owner"))
  df(0)(0).printSchema()
  println(df(0)(0).show())
//df(0)(0).select("owner").show()
  }

}

trait rulePredicates {}

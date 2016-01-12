package bi.megam

import org.apache.spark.sql.DataFrame

case class Engine(query: String, df: Set[DataFrame]) {
  private def applyRules(q: String) {
    //apply rules
  }

  def execute() {
    val newQuery = applyRules(query)
    //based on newQuery, manipulate the data
    //ex: date, revenue
    //take all date data and its respective mappings
  }



}

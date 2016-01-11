package bi.megam


case class Engine(query: String, data: String) {

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

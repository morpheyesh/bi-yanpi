package bi.megam

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD


case class Engine(query: String, df: List[List[DataFrame]]) extends rulePredicates {

private def singleSource(tables: List[DataFrame]): DataFrame = { //gets a single source with tables
//  tables.map(_.match )
   val p = natjoin(tables(0), tables(1))
   return p

}


  private def applyRules(q: String): List[String] = {
    //yet to apply rules
       return q.split(" ").toList


  }

  def execute(): Array[Any] = {
    val newQuery = applyRules(query)

   val d = df.map(x => singleSource(x))
   val f =  d(0).select(newQuery(0), newQuery(1)).rdd.map(r => r(0)).collect()

  return f
  }
}

trait rulePredicates {


  def natjoin(left: DataFrame, right: DataFrame): DataFrame = {
    val leftCols = left.columns
    val rightCols = right.columns

    val commonCols = leftCols.toSet intersect rightCols.toSet

    if(commonCols.isEmpty)
      left.limit(0).join(right.limit(0))
    else
      left
        .join(right, commonCols.map {col => left(col) === right(col) }.reduce(_ && _))
        .select(leftCols.collect { case c if commonCols.contains(c) => left(c) } ++
                leftCols.collect { case c if !commonCols.contains(c) => left(c) } ++
                rightCols.collect { case c if !commonCols.contains(c) => right(c) } : _*)
  }
}

package io.megam.meglytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._


case class Engine(query: String, df: List[List[DataFrame]]) extends rulePredicates {

private def singleSource(tables: List[DataFrame]): DataFrame = { //gets a single source with tables
val p =  natjoin(tables(0), tables(1))
   return p
}

  private def applyRules(q: String): List[String] = {
    //yet to apply rules
       return q.split(" ").toList
  }
def appd(x: String , f: List[Array[Any]]) = {
  println(f)
  var l = f.length
  var inc = 0
  if (inc <= l) {
    x ++ f(inc)
  }

}
  def execute(): List[Any] = {
    val newQuery = applyRules(query)
   val d = df.map(x => singleSource(x))
    val q = newQuery.map( x => d(0).select(x))
    val f = q.map(_.rdd.map(r => r(0)).collect())
    val n = newQuery.map(x => appd(x,f))
   println(n)
   println(f)
  return n
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
  def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
  val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
  a.select(columns: _*).unionAll(b.select(columns: _*))
}
}

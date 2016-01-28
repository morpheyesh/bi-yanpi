package io.megam.meglytics


import scalaz._
import Scalaz._
import scalaz.NonEmptyList._
import scala.collection.immutable._
import com.stackmob.scaliak._
import org.megam.common.riak.GSRiak
import org.megam.common.riak.GunnySack


case class ThrowErr(m: String) extends Exception(m)

trait RConn {

  val GatewayScaliakPool = Scaliak.clientPool(List("http://192.168.1.247"))

  def connection(bucketName: String) = new GSRiak("http://192.168.1.247", bucketName)(GatewayScaliakPool)

}

case class WorkBench(d: Any) extends RConn {

  val BUCKETNAME = "organizations"

  def fetch(key: String): ValidationNel[Throwable, GunnySack] = {

   val riak = connection(BUCKETNAME)
    (riak.fetch(key) leftMap { e: NonEmptyList[Throwable] =>
    throw e.head }).toValidationNel.flatMap {d: Option[GunnySack] =>
         d match {
           case Some(g) => {
             Validation.success[Throwable, GunnySack](g).toValidationNel
             }
            case None => Validation.failure[Throwable, GunnySack](new ThrowErr("Data not found")).toValidationNel
         }
      }
  }

  //get number of source and credentials
  def dSource(): Map[String,Map[String,List[String]]] = {

  //  val data = d.map(x => x.Map(x.get("source") -> Map(x.get("cred") -> List("tables"))))
     val data = Map("mysql" -> Map("user:pass" -> List("table1", "table2")))
     return data
  }
}

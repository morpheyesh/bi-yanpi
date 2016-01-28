/*
** Copyright [2013-2016] [Megam Systems]
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
** http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/

/**
 * @author morpheyesh
 *
 */

package io.megam.meglytics

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import java.sql.DriverManager
import io.megam.meglytics.Constants._



trait Connectors
object Connectors  {
  def apply(sc: SparkContext, source: String, cred: String, tables: String, db: String, ep: String, port: String): List[DataFrame] = source match {
    case "mysql" => return new Mysql(sc, cred, tables, db, ep, port).get()
    case "postgres" => return new Postgresql(sc, cred, tables, db, ep, port).get()
  }
}

//todo
//1. move SQLContext and get it implicitly
//2. passing same type of arguments to n number of sources
//3. move constants to a common place
//4. make a common mkEP

  private case class Mysql(sc: SparkContext,c: String, t: String, db: String, ep: String, port: String) extends Connectors {


    def get(): List[DataFrame] = {

      val url= MYSQL + "://" + ep + ":" + port + "/" + db //move this to a common ep builder fn
      val prop = new java.util.Properties

      prop.setProperty(USER, c.split(":")(0))
      prop.setProperty(PASSWORD, c.split(":")(1))
    //  t.split(" ").map(x => executeAnalysis(sc, x, url, prop))
      val pet = t.split(" ").map(x => new SQLContext(sc).read.jdbc(url, x, prop)).toList
    //  val pet = new SQLContext(sc).read.jdbc(url,"pet",prop)
      return pet
    }
  }
  private case class Postgresql(sc: SparkContext, c: String, t: String, db: String, ep: String, port: String) extends Connectors {


    def get(): List[DataFrame] = {

      val url= MYSQL + "://" + ep + ":" + port + "/" + db //move this to a common ep builder fn
      val prop = new java.util.Properties

      prop.setProperty(USER, c.split(":")(0))
      prop.setProperty(PASSWORD, c.split(":")(1))
    //  t.split(" ").map(x => executeAnalysis(sc, x, url, prop))
      val pet = t.split(" ").map(x => new SQLContext(sc).read.jdbc(url, x, prop)).toList
    //  val pet = new SQLContext(sc).read.jdbc(url,"pet",prop)
    //  println(pet.show())
      return pet
    }
  }

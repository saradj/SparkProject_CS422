package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //build buckets here

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here

    null
  }
}

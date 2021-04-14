package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here

    null
  }
}

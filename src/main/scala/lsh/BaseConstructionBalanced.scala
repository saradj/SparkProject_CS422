package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  //build buckets here

  def computeMinHashHistogram(queries : RDD[(String, Int)]) : Array[(Int, Int)] = {
    //compute histogram for target buckets of queries

    null
  }

  def computePartitions(histogram : Array[(Int, Int)]) : Array[Int] = {
    //compute the boundaries of bucket partitions

    null
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here

    null
  }
}
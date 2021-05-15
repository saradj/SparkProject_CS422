package lsh

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
class BaseConstructionBroadcast(sqlContext: SQLContext,
                                data: RDD[(String, List[String])],
                                seed: Int)
    extends Construction
    with Serializable {
  //build buckets here

  val minhash = new MinHash(seed)

  val data_modified: RDD[(Int, Set[String])] = minhash
    .execute(data)
    .map(x => (x._2, x._1))
    .groupByKey()
    .map(x => (x._1, x._2.toSet))

  val rddMap: Map[Int, Set[String]] = data_modified.collect().toMap
  val bc: Broadcast[Map[Int, Set[String]]] =
    SparkContext.getOrCreate().broadcast(rddMap)

  override def eval(
      queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    val res: RDD[(String, Set[String])] = minhash
      .execute(queries)
      .map(x => (x._2, x._1))
      .map(x => (x._2, bc.value.getOrElse(x._1, Set[String]())))
    res
  }
}

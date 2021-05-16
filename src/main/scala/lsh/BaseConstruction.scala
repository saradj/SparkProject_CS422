package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
class BaseConstruction(sqlContext: SQLContext,
                       data: RDD[(String, List[String])],
                       seed: Int)
    extends Construction {
  //build buckets here
  val minhash = new MinHash(seed)
  val data_modified = minhash
    .execute(data)
    .map(x => (x._2, x._1))
    .groupBy(_._1)
    .map { case (h, films) => (h, films.map(_._2).toSet) }.cache()


  override def eval(
      queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    val rdd_modified: RDD[(Int, String)] =
      minhash.execute(queries).map(x => (x._2, x._1)).cache()

   rdd_modified
      .join(data_modified)
      .map { case (_, t) => t }
  }

}

package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext,
              data: RDD[(String, List[String])],
              threshold: Double)
    extends Construction
    with Serializable {
  override def eval(
      rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    val temp: RDD[(String, String, Double)] = rdd
      .cartesian(data)
      .map(
        x =>
          (x._1._1,
           x._2._1,
           (x._2._2.toSet
             .intersect(x._1._2.toSet)
             .size)
             .toDouble / (x._2._2.toSet.union(x._1._2.toSet).size.toDouble)))

    val res = temp
      .filter(x => x._3 > threshold)
      .map(x => (x._1, x._2))
      .groupByKey()
      .map(x => (x._1, x._2.toSet))

    res
  }
}

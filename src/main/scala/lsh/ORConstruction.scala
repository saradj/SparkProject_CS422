package lsh

import org.apache.spark.rdd.RDD

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(
      rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    val duplKeys = rdd.keys.map(x => (x, x))

    children
      .map(_.eval(rdd))
      .reduce((rddA, rddB) => rddA.union(rddB))
      .reduceByKey(_.union(_))
      .join(duplKeys)
      .map(x => (x._2._2, x._2._1))

  }

}

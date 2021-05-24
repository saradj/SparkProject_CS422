package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.{ListBuffer}

class BaseConstructionBalanced(sqlContext: SQLContext,
                               data: RDD[(String, List[String])],
                               seed: Int,
                               partitions: Int)
    extends Construction {
  //build buckets here
  val minhash = new MinHash(seed)
  val data_hashed: RDD[(Int, Set[String])] = minhash
    .execute(data)
    .map(x => (x._2, x._1))
    .groupBy(_._1)
    //print()
    .map { case (h, films) => (h, films.map(_._2).toSet) }.cache()

  def computeMinHashHistogram(queries: RDD[(String, Int)])
    : Array[(Int, Int)] = { //returns tuple hash with numb of movies
    val k = queries.groupBy(_._2).map(x => (x._1, x._2.size)).sortByKey(true) //.count()
    k.collect()
  }

  def computePartitions(histogram: Array[(Int, Int)]): Array[Int] = {
    //compute the boundaries of bucket partitions
    var res: Array[Int] = Array()
    if (histogram.isEmpty) return res
    res = res :+ histogram(0)._1
    var cnt = histogram(0)._2
    val thres = math.ceil(histogram.map(_._2).sum / partitions)
    for ((idx, nb) <- histogram.drop(1)) {
      if (nb + cnt > thres) {
        res = res :+ idx
        cnt = nb
      } else {
        cnt += nb
      }
    }
    res
  }

  override def eval(
      queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here

    val query_hashed = minhash.execute(queries).cache() //.map(x=>(x._2,x._1))
    val histogram = computeMinHashHistogram(query_hashed) //check??
    val partitions_queries = computePartitions(histogram)

    val getPartition = (hash: Int) =>
      partitions_queries
        .takeWhile(threshold => threshold <= hash)
        .lastOption
        .getOrElse(partitions_queries(0))

    val data_modified = data_hashed
      .map(x => (getPartition(x._1), x)).cache()

    val query_modified =
      query_hashed.map(x => (x._2, x._1)).map(x => (getPartition(x._1), x)).cache()

    query_modified
      .cogroup(data_modified)
      .mapPartitions(
        grouped_iter => {
          var res = new ListBuffer[(String, Set[String])]()
          while (grouped_iter.hasNext) {
            val grouped = grouped_iter.next()
            val queries_on_partition = grouped._2._1 //.sortBy(_._1)
            val buckets_on_partition = grouped._2._2.toMap //toList.sortBy(_._1)
            for (queryTuple <- queries_on_partition) {
              val set = buckets_on_partition.get(queryTuple._1).getOrElse(Set())
              res = res :+ (queryTuple._2, set)
            }
          }
          res.iterator
        }
      )
  }
}

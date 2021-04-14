package lsh
import org.apache.spark.rdd.RDD

class ANDConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute AND construction results here

    null
  }
}

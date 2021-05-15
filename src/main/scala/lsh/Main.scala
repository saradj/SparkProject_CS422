package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def generate(sc: SparkContext,
               input_file: String,
               output_file: String,
               fraction: Double): Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth: RDD[(String, Set[String])],
             lsh_truth: RDD[(String, Set[String])]): Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble / x._3.toDouble, 1))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    val avg_recall = recall_vec._1 / recall_vec._2

    avg_recall
  }

  def precision(ground_truth: RDD[(String, Set[String])],
                lsh_truth: RDD[(String, Set[String])]): Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble / x._3.toDouble, 1))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    val avg_precision = precision_vec._1 / precision_vec._2

    avg_precision
  }

  def construction1(sqlContext: SQLContext,
                    rdd_corpus: RDD[(String, List[String])]): Construction = {
    val lsh1 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 0)
    val lsh2 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 1)
    val lsh3 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 2)
    val lsh4 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 3)

    val lsh = new ANDConstruction(List(lsh1, lsh2, lsh3, lsh4))
    return lsh
  }

  def construction2(sqlContext: SQLContext,
                    rdd_corpus: RDD[(String, List[String])]): Construction = {
    val lsh1 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 0)
    val lsh2 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 1)
    val lsh3 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 2)
    val lsh4 = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 3)

    val lsh = new ORConstruction(List(lsh1, lsh2, lsh3, lsh4))
    lsh
  }

  def timeMS(toLaunch: () => Unit): Double = {
    val start = System.nanoTime()
    toLaunch()
    val stop = System.nanoTime()
    (stop - start) / 1e6
  }

  def eval_query(sc: SparkContext,
                 sqlContext: SQLContext,
                 rdd_corpus: RDD[(String, List[String])],
                 query_file: String): Unit = {

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction =
      new ExactNN(sqlContext, rdd_corpus, threshold = 0.3)

    var ground: RDD[(String, Set[String])] = null
    /*
    val timeBase = getTimingInMs(() => {

      ground = exact.eval(rdd_query).persist()
      ground.count()
    })
    println("--- Time ExactNN: " + timeBase + "ms")


     */
    val nb_partitions = 8

    var lshBaseTimes: List[Double] = List()
    var lshBaseTimesInit: List[Double] = List()
    var lshBasePrecision: List[Double] = List()
    var lshBaseRecall: List[Double] = List()
    var lshBaseBalancedPrecision: List[Double] = List()
    var lshBaseBalancedRecall: List[Double] = List()
    var lshBaseBroadcastPrecision: List[Double] = List()
    var lshBaseBroadcastRecall: List[Double] = List()
    var meanBase: List[Double] = List()
    var meanBaseBalanced: List[Double] = List()
    var meanBaseBroadcast: List[Double] = List()
    val nb_iter = 5
    val nb_iter_dist = 1
    for (i <- (0 until nb_iter)) {
      var lsh: Construction = null
      val timeLshBdcstInit = timeMS(() => {
        lsh = new BaseConstruction(sqlContext, rdd_corpus, i)
      })
      lshBaseTimesInit = lshBaseTimesInit :+ timeLshBdcstInit
      // println("--- Time LSH init: " + timeLshBdcstInit + "ms")
      var baseRes: RDD[(String, Set[String])] = null
      val timeLsh = timeMS(() => {
        baseRes = lsh.eval(rdd_query).persist()
        baseRes.count()
      })
      lshBaseTimes = lshBaseTimes :+ timeLsh
      /*
      lshBasePrecision = lshBasePrecision :+ precision(ground, baseRes)
      lshBaseRecall = lshBaseRecall :+ recall(ground, baseRes)

       */

      val (meanList, mean_dist) =
        computeAverageDist(baseRes, rdd_query, rdd_corpus)
      //meanBase = meanBase :+ meanList
      meanBase = meanBase :+ mean_dist
      //println("--- Time LSH eval: " + timeLsh + "ms")
    }
    println(
      " base constr avg time for " + nb_iter + " iterations " + mean(
        lshBaseTimes))
    println(
      " base constr avg time init for " + nb_iter + " iterations " + mean(
        lshBaseTimesInit))
    println("base constr avg times: " + lshBaseTimes)
    println("base constr avg times Init: " + lshBaseTimesInit)
//    println(
//      " base constr precision for " + nb_iter + " iterations " + mean(
//        lshBasePrecision))
//
//    println(
//      " base constr recall for " + nb_iter + " iterations " + mean(
//        lshBaseRecall))
//    println(
//      " base constr precision for " + nb_iter + " iterations " + (lshBasePrecision))
//    println(
//      " base constr recall for " + nb_iter + " iterations " + (lshBaseRecall))

    println(
      " base constr mean dist for " + nb_iter + " iterations " + mean(meanBase))
    println(
      " base constr mean dist for " + nb_iter + " iterations " + (meanBase))

    /*
        var lshBaseBalancedTimes: List[Double] = List()
        var lshBaseTimesBalancedInit: List[Double] = List()

        //val nb_iter = 20

        for (i <- (0 until nb_iter)) {
          var lsh: Construction = null
          val timeLshBdcstInit = getTimingInMs(() => {
            lsh =
              new BaseConstructionBalanced(sqlContext, rdd_corpus, i, nb_partitions)
          })
          lshBaseTimesBalancedInit = lshBaseTimesBalancedInit :+ timeLshBdcstInit
          //println("--- Time LSH init: " + timeLshBdcstInit + "ms")
          var baseResBalance: RDD[(String, Set[String])] = null
          val timeLsh = getTimingInMs(() => {
            baseResBalance = lsh.eval(rdd_query).persist()
            baseResBalance.count()
          })
          lshBaseBalancedTimes = lshBaseBalancedTimes :+ timeLsh

          lshBaseBalancedPrecision = lshBaseBalancedPrecision :+ precision(
            ground,
            baseResBalance)
          lshBaseBalancedRecall = lshBaseBalancedRecall :+ recall(ground,
                                                                  baseResBalance)


          val (meanList,
          mean_dist) =
            computeAverageDist(baseResBalance, rdd_query, rdd_corpus)
          //meanBase = meanBase :+ meanList
          meanBaseBalanced = meanBaseBalanced:+ mean_dist
         // stdBaseBalanced = stdBaseBalanced :+ stdDistDiffTotPointwise
          //println("--- Time LSH eval: " + timeLsh + "ms")
        }
        println(
          " base constr balanced avg time for " + nb_iter + " iterations " + mean(
            lshBaseBalancedTimes))
        println("base constr balanced avg times: " + lshBaseBalancedTimes)
        println(
          "base constr balanced avg times Init mean : " + mean(
            lshBaseTimesBalancedInit))
        println("base constr balanced avg times Init: " + lshBaseTimesBalancedInit)

        println(
          " base constr balanced precision for " + nb_iter + " iterations " + mean(
            lshBaseBalancedPrecision))

        println(
          " base constr balanced recall for " + nb_iter + " iterations " + mean(
            lshBaseBalancedRecall))

        println(
          " base constr balanced avg time init for " + nb_iter + " iterations " + mean(
            lshBaseTimesBalancedInit))
        println(
          " base constr balanced precision for " + nb_iter + " iterations " + (lshBaseBalancedPrecision))

        println(
          " base constr balanced recall for " + nb_iter + " iterations " + (lshBaseBalancedRecall))

        println(
          " base constr balanced mean dist for " + nb_iter + " iterations " + mean(
            meanBaseBalanced))

        println(
          " base constr balanced mean dist for " + nb_iter + " iterations " + (meanBaseBalanced))


        var lshBroadcastTimes: List[Double] = List()
        var lshBroadcastInitTimes: List[Double] = List()

        for (i <- (0 until nb_iter)) {
          var lsh: Construction = null
          val timeLshBdcstInit = getTimingInMs(() => {
            lsh = new BaseConstructionBroadcast(sqlContext, rdd_corpus, i)
          })
          lshBroadcastInitTimes = lshBroadcastInitTimes :+ timeLshBdcstInit

          var baseResBroad: RDD[(String, Set[String])] = null
          val timeLsh = getTimingInMs(() => {
            baseResBroad = lsh.eval(rdd_query).persist()
            baseResBroad.count()
          })
          lshBroadcastTimes = lshBroadcastTimes :+ timeLsh

          lshBaseBroadcastPrecision = lshBaseBroadcastPrecision :+ precision(
            ground,
            baseResBroad)
          lshBaseBroadcastRecall = lshBaseBroadcastRecall :+ recall(ground,
                                                                    baseResBroad)


          val (meanList,
          mean_dist) =
            computeAverageDist(baseResBroad, rdd_query, rdd_corpus)
          //meanBase = meanBase :+ meanList
          meanBaseBroadcast = meanBaseBroadcast:+ mean_dist
        }

        println(
          " base constr broadcast avg time for " + nb_iter + " iterations " + mean(
            lshBroadcastTimes))
        println(
          " base constr broadcast avg time init for " + nb_iter + " iterations " + mean(
            lshBroadcastInitTimes))
        println("base constr broadcast avg times: " + lshBroadcastTimes)

        println("base constr broadcast avg times Init: " + lshBroadcastInitTimes)
        println(
          " base constr broadcast precision for " + nb_iter + " iterations " + mean(
            lshBaseBroadcastPrecision))

        println(
          " base constr broadcast recall for " + nb_iter + " iterations " + mean(
            lshBaseBroadcastRecall))

        println(
          " base constr broadcast precision for " + nb_iter + " iterations " + (lshBaseBroadcastPrecision))

        println(
          " base constr broadcast recall for " + nb_iter + " iterations " + (lshBaseBroadcastRecall))
        println(
          " base constr broadcast mean dist for " + nb_iter + " iterations " + mean(
            meanBaseBroadcast))
        println(
          " base constr broadcast mean dist for " + nb_iter + " iterations " + (meanBaseBroadcast))
     */
    var andBroadcastTimes: List[Double] = List()
    var andBroadcastInitTimes: List[Double] = List()
    var orBroadcastTimes: List[Double] = List()
    var orBroadcastInitTimes: List[Double] = List()

    var andBaseBroadcastPrecision: List[Double] = List()
    var andBaseBroadcastRecall: List[Double] = List()
    var andmeanBaseBroadcast: List[Double] = List()

    var orBaseBroadcastPrecision: List[Double] = List()
    var orBaseBroadcastRecall: List[Double] = List()
    var ormeanBaseBroadcast: List[Double] = List()

    for (i <- (0 until nb_iter)) {
      var lsh: Construction = null
      val timeLshBdcstInit = timeMS(() => {
        lsh = construction1(sqlContext, rdd_corpus)
      })
      andBroadcastInitTimes = andBroadcastInitTimes :+ timeLshBdcstInit

      var baseResBroad: RDD[(String, Set[String])] = null
      val timeLsh = timeMS(() => {
        baseResBroad = lsh.eval(rdd_query).persist()
        baseResBroad.count()
      })
      andBroadcastTimes = andBroadcastTimes :+ timeLsh
      /*
      andBaseBroadcastPrecision = andBaseBroadcastPrecision :+ precision(
        ground,
        baseResBroad)
      andBaseBroadcastRecall = andBaseBroadcastRecall :+ recall(ground,
                                                                baseResBroad)

       */
      val (meanList, mean_dist) =
        computeAverageDist(baseResBroad, rdd_query, rdd_corpus)
      //meanBase = meanBase :+ meanList
      andmeanBaseBroadcast = andmeanBaseBroadcast :+ mean_dist

    }

    println(
      " and constr broadcast avg time for " + nb_iter + " iterations " + mean(
        andBroadcastTimes))
    println(
      " and constr broadcast avg time init for " + nb_iter + " iterations " + mean(
        andBroadcastInitTimes))
    println("and constr broadcast avg times: " + andBroadcastTimes)

    println("and constr broadcast avg times Init: " + andBroadcastInitTimes)
    println(
      " and constr broadcast precision for " + nb_iter + " iterations " + mean(
        andBaseBroadcastPrecision))

    println(
      " and constr broadcast recall for " + nb_iter + " iterations " + mean(
        andBaseBroadcastRecall))

    println(
      " and constr broadcast precision for " + nb_iter + " iterations " + (andBaseBroadcastPrecision))

    println(
      " and constr broadcast recall for " + nb_iter + " iterations " + (andBaseBroadcastRecall))
    println(
      " and constr broadcast mean dist for " + nb_iter + " iterations " + mean(
        andmeanBaseBroadcast))
    println(
      " and constr broadcast mean dist for " + nb_iter + " iterations " + (andmeanBaseBroadcast))

    for (i <- (0 until nb_iter)) {
      var lsh: Construction = null
      val timeLshBdcstInit = timeMS(() => {
        lsh = construction2(sqlContext, rdd_corpus)
      })
      orBroadcastInitTimes = orBroadcastInitTimes :+ timeLshBdcstInit

      var baseResBroad: RDD[(String, Set[String])] = null
      val timeLsh = timeMS(() => {
        baseResBroad = lsh.eval(rdd_query).persist()
        baseResBroad.count()
      })
      orBroadcastTimes = orBroadcastTimes :+ timeLsh
      /*
      orBaseBroadcastPrecision = orBaseBroadcastPrecision :+ precision(
        ground,
        baseResBroad)
      orBaseBroadcastRecall = orBaseBroadcastRecall :+ recall(ground,
                                                              baseResBroad)

       */
      val (meanList, mean_dist) =
        computeAverageDist(baseResBroad, rdd_query, rdd_corpus)
      //meanBase = meanBase :+ meanList
      ormeanBaseBroadcast = ormeanBaseBroadcast :+ mean_dist

    }
    println(
      " or constr broadcast avg time for " + nb_iter + " iterations " + mean(
        orBroadcastTimes))
    println(
      " or constr broadcast avg time init for " + nb_iter + " iterations " + mean(
        orBroadcastInitTimes))
    println("or constr broadcast avg times: " + orBroadcastTimes)

    println("or constr broadcast avg times Init: " + orBroadcastInitTimes)
    println(
      " or constr broadcast precision for " + nb_iter + " iterations " + mean(
        orBaseBroadcastPrecision))

    println(
      " or constr broadcast recall for " + nb_iter + " iterations " + mean(
        orBaseBroadcastRecall))

    println(
      " or constr broadcast precision for " + nb_iter + " iterations " + (orBaseBroadcastPrecision))

    println(
      " or constr broadcast recall for " + nb_iter + " iterations " + (orBaseBroadcastRecall))
    println(
      " or constr broadcast mean dist for " + nb_iter + " iterations " + mean(
        ormeanBaseBroadcast))
    println(
      " or constr broadcast mean dist for " + nb_iter + " iterations " + (ormeanBaseBroadcast))
    //--------------------------------Calculating distance----------------------------------------

  }

  def mean[T: Numeric](xs: Iterable[T]): Double =
    xs.sum.asInstanceOf[Double] / xs.size

  def computeAverageDist(nn: RDD[(String, Set[String])],
                         queryRDD: RDD[(String, List[String])],
                         corpusRdd: RDD[(String, List[String])])
    : (RDD[(String, Double)], Double) = {
    val mapQuery = queryRDD.collectAsMap()
    val mapCorpus = corpusRdd.collectAsMap()

    def jaccardDistance(movieKeys: List[String], data: List[String]): Double = {
      val movieSet = movieKeys.toSet
      val dataSet = data.toSet
      movieSet.intersect(dataSet).size.toDouble / movieSet
        .union(dataSet)
        .size
        .toDouble
    }

    val distances =
      nn.map {
          case (movie, neighbors: Set[String]) => //println("Ever in this case???")
            (movie, mapQuery(movie), neighbors.map(mapCorpus))
        }
        .map {
          case (movie, movieKeys: List[String], neighKeys: Set[List[String]]) =>
            (movie, neighKeys.map(nKey => jaccardDistance(movieKeys, nKey)))
        }
        .map {
          case (movie, neighDist: Set[Double]) => //println("Or here ???")
            (movie, mean(neighDist))
        }

    val meanDist = distances.map(_._2).mean()
    (distances, meanDist)
  }

  def main(args: Array[String]) {
    import java.io.{FileOutputStream, PrintStream}
    val out = new PrintStream(new FileOutputStream("output.txt"))
    System.setOut(out)
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val map_cl_q = Map(
      "/corpus-1" -> List("/queries-1-2",
                          "/queries-1-2-skew",
                          "/queries-1-10",
                          "/queries-1-10-skew"),
           "/corpus-10" -> List(
        "/queries-10-2",
                           "/queries-10-2-skew",
        "/queries-10-10",
         "/queries-10-10-skew"),
      "/corpus-20" -> List("/queries-20-2",
                           "/queries-20-2-skew",
                           "/queries-20-10",
                           "/queries-20-10-skew")
    )
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //val corpus_file = new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath
    for (map_entry <- map_cl_q) {
      val corpus_file =
        //  new File(getClass.getResource(map_entry._1 + ".csv/part-00000").getFile).getPath
        "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422/sdjambaz/data" + map_entry._1 + ".csv/part-00000"
      println("*****************" + map_entry._1 + "********************")
      val rdd_corpus: RDD[(String, List[String])] = sc
        .textFile(corpus_file)
        //.textFile("/user/cs422/lsh-corpus-large.csv")
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))
      for (q_name <- map_entry._2) {
        println("*****************" + q_name + "********************")
        val query_file =
          "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422/sdjambaz/data" + q_name + ".csv/part-00000"
        // new File(getClass.getResource(q_name + ".csv/part-00000").getFile).getPath

        val start = System.nanoTime
        eval_query(sc, sqlContext, rdd_corpus, query_file)
        val stop = System.nanoTime
        println("Duration for query " + q_name + ": " + (stop - start))
        println()
      }
      println()
      println()
    }
  }
}

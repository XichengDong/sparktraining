package org.training.spark.core

import org.apache.spark._

import scala.collection.immutable.HashSet

/**
 * 得分最高的10部电影；看过电影最多的前10个人；女性看多最多的10部电影；男性看过最多的10部电影
 */
object TopKMovieAnalyzer {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1) {
      dataPath = args(1)
    }

    // Create a SparContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("TopKMovieAnalyzer")
    val sc = new SparkContext(conf)

    /**
     * Step 1: Create RDDs
     */
    val DATA_PATH = dataPath

    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")

    /**
     * Step 2: Extract columns from RDDs
     */

    //users: RDD[(userID, movieID, score)]
    val ratings = ratingsRdd.map(_.split("::")).map { x =>
      (x(0), x(1), x(2))
    }.cache


    /**
     * Step 3: analyze result
     */

    val topKScoreMostMovie = ratings.map{x =>
      (x._2, (x._3.toInt, 1))
    }.reduceByKey { (v1, v2) =>
      (v1._1 + v2._1, v1._2 + v2._2)
    }.map { x =>
      (x._2._1.toFloat / x._2._2.toFloat, x._1)
    }.sortByKey(false).
        take(10).
        foreach(println)


    val topKmostPerson = ratings.map{ x =>
      (x._1, 1)
    }.reduceByKey(_ + _).
        map(x => (x._2, x._1)).
        sortByKey(false).
        take(10).
        foreach(println)

    sc.stop()
  }
}

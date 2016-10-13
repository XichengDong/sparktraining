package org.training.spark.core

import org.apache.spark._

import scala.collection.immutable.HashSet

/**
 * 年龄段在“18-24”的男性年轻人，最喜欢看哪10部电影
 */
object PopularMovieAnalyzer {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1) {
      dataPath = args(1)
    }

    // Create a SparContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("PopularMovieAnalyzer")
    val sc = new SparkContext(conf)

    /**
     * Step 1: Create RDDs
     */
    val DATA_PATH = dataPath
    val USER_AGE = "18"


    val usersRdd = sc.textFile(DATA_PATH + "users.dat")
    val moviesRdd = sc.textFile(DATA_PATH + "movies.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")

    /**
     * Step 2: Extract columns from RDDs
     */


    //users: RDD[(userID, age)]
    val users = usersRdd.map(_.split("::")).map { x =>
      (x(0), x(2))
    }.filter(_._2.equals(USER_AGE))

    //Array[String]
    val userlist = users.map(_._1).collect()

    //broadcast
    val userSet = HashSet() ++ userlist
    val broadcastUserSet = sc.broadcast(userSet)


    /**
     * Step 3: map-side join RDDs
     */

    val topKmovies = ratingsRdd.map(_.split("::")).map{ x =>
      (x(0), x(1))
    }.filter { x =>
      broadcastUserSet.value.contains(x._1)
    }.map{ x=>
      (x._2, 1)
    }.reduceByKey(_ + _).map{ x =>
      (x._2, x._1)
    }.sortByKey(false).map{ x=>
      (x._2, x._1)
    }.take(10)

    /**
     * Transfrom filmID to fileName
     */
    val movieID2Name = moviesRdd.map(_.split("::")).map { x =>
      (x(0), x(1))
    }.collect().toMap

    topKmovies.map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)

    println(System.currentTimeMillis())

    sc.stop()
  }
}

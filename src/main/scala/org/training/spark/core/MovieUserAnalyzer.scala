package org.training.spark.core

import org.apache.spark._

/**
 * 看过“Lord of the Rings, The (1978)”用户和年龄性别分布
 */
object MovieUserAnalyzer {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1) {
      dataPath = args(1)
    }

    // Create a SparContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalyzer")
    val sc = new SparkContext(conf)

    /**
     * Step 1: Create RDDs
     */
    val DATA_PATH = dataPath
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"
    val MOVIE_ID = "2116"


    val usersRdd = sc.textFile(DATA_PATH + "users.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")


    /**
     * Step 2: Extract columns from RDDs
     */


    //users: RDD[(userID, (gender, age))]
    val users = usersRdd.map(_.split("::")).map { x =>
      (x(0), (x(1), x(2)))
    }

    //rating: RDD[Array(userID, movieID, ratings, timestamp)]
    val rating = ratingsRdd.map(_.split("::"))

    //usermovie: RDD[(userID, movieID)]
    val usermovie = rating.map{ x =>
      (x(0), x(1))
    }.filter(_._2.equals(MOVIE_ID))

    /**
     * Step 3: join RDDs
     */

    //useRating: RDD[(userID, (movieID, (gender, age))]
    val userRating = usermovie.join(users)

    //userRating.take(1).foreach(print)


    //movieuser: RDD[(movieID, (movieTile, (gender, age))]
    val userDistribution = userRating.map { x =>
      (x._2._2, 1)
    }.reduceByKey(_ + _)

    userDistribution.foreach(println)

    sc.stop()
  }
}
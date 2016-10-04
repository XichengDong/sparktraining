package org.training.spark.reco.offline

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.training.spark.proto.Spark.{ItemList, ItemSimilarity, ItemSimilarities}
import org.training.spark.util.RedisClient

object MoviesSimilarityGenerator {
  def main(args: Array[String]) {
    var masterUrl = "local[4]"
    var dataPath = "data/ml-1m/ratings.dat"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    // Create a SparContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("FilmsSimilarityGenerator")
    val sc = new SparkContext(conf)

    val inputFile = dataPath
    val threshold = 0.1

    // Load and parse the data file.
    val rows = sc.textFile(inputFile).map(_.split("::")).map { p =>
      (p(0).toLong, p(1).toInt, p(2).toDouble)
    }

    val maxMovieId = rows.map(_._2).max() + 1

    val rowRdd = rows.map { p =>
      (p._1, (p._2, p._3))
    }.groupByKey().map { kv =>
      Vectors.sparse(maxMovieId, kv._2.toSeq)
    }.cache()

    val mat = new RowMatrix(rowRdd)

    println(s"mat row/col number: ${mat.numRows()}, ${mat.numCols()}")

    // Compute similar columns perfectly, with brute force.
    val similarities = mat.columnSimilarities(0.1)

    // Save movie-movie similarity to redis
    similarities.entries.map { case MatrixEntry(i, j, u) =>
      (i, (j, u))
    }.groupByKey(2).map { kv =>
      (kv._1, kv._2.toSeq.sortWith(_._2 > _._2).take(20).toMap)
    }.mapPartitions { iter =>
      val jedis = RedisClient.pool.getResource
      iter.map { case (i, j) =>
        val key = ("II:%d").format(i)
        val builder = ItemSimilarities
            .newBuilder()
        j.foreach { item =>
          val itemSimilarity = ItemSimilarity.newBuilder().setItemId(item._1).setSimilarity(item._2)
          builder.addItemSimilarites(itemSimilarity)
        }
        val value = builder.build()
        println(s"key:${key},value:${value.toString}")
        jedis.set(key, new String(value.toByteArray))
      }
    }.count

    // Save user-movie similarity to redis
    rows.map { case (i, j, k) =>
      (i, j)
    }.groupByKey(2).mapPartitions { iter =>
      val jedis = RedisClient.pool.getResource
      iter.map { case (i, j) =>
        val key = ("UI:%d").format(i)
        val builder = ItemList.newBuilder()
        j.foreach { id =>
          builder.addItemIds(id)
        }
        val value = builder.build()
        println(s"key:${key},value:${value.toString}")
        jedis.append(key, new String(value.toByteArray))
      }
    }.count

      sc.stop()
    }
  }

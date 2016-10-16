package org.training.spark.optimize

import java.util.concurrent.CountDownLatch

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xicheng.dong on 10/16/16.
 * --num-executors 4
 */
object MultipleJobs {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.minsize", "1000000000")

    /**
     * Step 1: Prepare RDDs
     */
    val DATA_PATH = "/home/hadoop/input/data_wide_3.txt"

    val peopleTxtRdd = sc.textFile(DATA_PATH)

    val latch = new CountDownLatch(2)

    new Thread() {
      new Runnable {
        def run = {
          println(peopleTxtRdd.count)
          latch.countDown()
        }
      }
    }.start()

    new Thread() {
      new Runnable {
        def run = {
          println(peopleTxtRdd.take(10))
          latch.countDown()
        }
      }
    }.start()

    latch.await()
    sc.stop()
  }
}

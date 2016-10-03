// scalastyle:off println
package org.training.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An example app for linear regression. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.LinearRegression
 * }}}
 * A synthetic dataset can be found at `data/mllib/sample_linear_regression_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LinearRegressionExample {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var dataPath = "data/mllib/sample_linear_regression_data.txt"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1) {
      dataPath = args(1)
    }

    // Create a SparContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("LinearRegressionExample")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = MLUtils.loadLibSVMFile(sc, dataPath).cache()

    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    examples.unpersist(blocking = false)

    val updater = new SimpleUpdater()
    val numIterations = 10
    val stepSize = 10
    val regParam = 0.1

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(numIterations)
      .setStepSize(stepSize)
      .setUpdater(updater)
      .setRegParam(regParam)

    val model = algorithm.run(training)

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val loss = predictionAndLabel.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numTest)

    println(s"Test RMSE = $rmse.")

    sc.stop()
  }
}
// scalastyle:on println

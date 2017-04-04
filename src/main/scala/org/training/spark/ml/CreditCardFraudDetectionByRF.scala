package org.training.spark.ml

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql._

object CreditCardFraudDetectionByRF {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .master("local")
        .appName("CreditCardFraudDetectionByRF")
        .getOrCreate()

    val maxDepth: Int = 5
    val maxBins: Int = 32
    val minInstancesPerNode: Int = 1
    val minInfoGain: Double = 0.0
    val fracTest: Double = 0.2
    val cacheNodeIds: Boolean = false
    val checkpointDir: Option[String] = None
    val checkpointInterval: Int = 10

    val (training: DataFrame, test: DataFrame) =
      MLUtils.loadDatasets("data/creditdata", fracTest)

    val dt = new RandomForestClassifier()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setMaxDepth(maxDepth)
        .setMaxBins(maxBins)
        .setMinInstancesPerNode(minInstancesPerNode)
        .setMinInfoGain(minInfoGain)
        .setCacheNodeIds(cacheNodeIds)
        .setCheckpointInterval(checkpointInterval)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val rfModel = dt.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    if (rfModel.totalNumNodes < 30) {
      println(rfModel.toDebugString) // Print full model.
    } else {
      println(rfModel) // Print model summary.
    }

    println("Training data results:")
    MLUtils.evaluateModel(rfModel, training, "label")
    println("Test data results:")
    MLUtils.evaluateModel(rfModel, test, "label")

    spark.stop()
  }
}
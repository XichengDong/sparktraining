package org.training.spark.ml

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.sql._

object CreditCardFraudDetectionByDT {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .master("local")
        .appName("CreditCardFraudDetectionByDT")
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

    val dt = new DecisionTreeClassifier()
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
    val dtModel = dt.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    if (dtModel.numNodes < 20) {
      println(dtModel.toDebugString) // Print full model.
    } else {
      println(dtModel) // Print model summary.
    }

    println("Training data results:")
    MLUtils.evaluateModel(dtModel, training, "label")
    println("Test data results:")
    MLUtils.evaluateModel(dtModel, test, "label")

    spark.stop()
  }
}

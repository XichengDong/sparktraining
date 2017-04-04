package org.training.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql._

object CreditCardFraudDetectionByLR {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .master("local")
        .appName("CreditCardFraudDetectionByLR")
        .getOrCreate()

    val regParam: Double = 0.0
    val elasticNetParam: Double = 0.0
    val maxIter: Int = 100
    val fitIntercept: Boolean = true
    val tol: Double = 1E-6
    val fracTest: Double = 0.2

    val (training: DataFrame, test: DataFrame) =
      MLUtils.loadDatasets("data/creditdata", fracTest)

    val lor = new LogisticRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setRegParam(regParam)
        .setElasticNetParam(elasticNetParam)
        .setMaxIter(maxIter)
        .setTol(tol)
        .setFitIntercept(fitIntercept)
        //.setWeightCol("classWeight")

    val startTime = System.nanoTime()
    val lorModel = lor.fit(MLUtils.balanceDataset(training))
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    // Print the weights and intercept for linear regression.
    println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")

    println("Training data results:")
    MLUtils.evaluateModel(lorModel, training, "label")
    println("Test data results:")
    MLUtils.evaluateModel(lorModel, test, "label")

    spark.stop()
  }
}
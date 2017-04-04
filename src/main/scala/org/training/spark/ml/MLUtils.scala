package org.training.spark.ml

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.udf

object MLUtils {
  def loadDatasets(
    input: String,
    fracTest: Double): (DataFrame, DataFrame) = {
    val spark = SparkSession
        .builder
        .getOrCreate()

    import spark.implicits._
    val inputDF = spark.read.option("header","true").csv(input)
    val dataset = inputDF.map {row =>
      val seq = row.toSeq
      val data = seq.drop(1).dropRight(2).map(_.asInstanceOf[String].toDouble)
      (Vectors.dense(data.toArray), seq.last.asInstanceOf[String].toDouble)
    }.toDF("features", "label")

    val datasets = dataset.randomSplit(Array(1.0 - fracTest, fracTest), seed = 12345)

    val training = datasets(0).cache()
    val test = datasets(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println("Loaded data:")
    println(s"  numTraining = $numTraining, numTest = $numTest")
    println(s"  numFeatures = $numFeatures")

    (training, test)
  }

  def evaluateModel(
    model: Transformer,
    data: DataFrame,
    labelColName: String): Unit = {
    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
    val metrics = new MulticlassMetrics(predictions.zip(labels))
    val (precision1, precision2) = (metrics.precision(1.0), metrics.precision(0.0))
    println(s"label: 1.0, precision:$precision1, label:0.0,precision:$precision2")
    val (recall1, recall2) = (metrics.recall(1.0), metrics.recall(0.0))
    println(s"label: 1.0, recall:$recall1, label:0.0,recall:$recall2")

    println("Confusion Matrix:")
    val confusionMatrics = metrics.confusionMatrix
    println(metrics.confusionMatrix.toString(2, 20))

    val binaryMetrics = new BinaryClassificationMetrics(predictions.zip(labels), 100)
    val roc = binaryMetrics.roc()
    roc.foreach { case (t: Double, v: Double) =>
      println(s"threshold: $t, value: $v")
    }
  }

  def balanceDataset(dataset: DataFrame): DataFrame = {
    // Re-balancing (weighting) of records to be used in the logistic loss objective function
    val numNegatives = dataset.filter(dataset("label") === 0).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 0.0) {
        1 * balancingRatio
      }
      else {
        (1 * (1.0 - balancingRatio))
      }
    }

    val weightedDataset = dataset.withColumn("classWeight", calculateWeights(dataset("label")))
    weightedDataset
  }
}
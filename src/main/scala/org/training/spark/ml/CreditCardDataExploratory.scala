package org.training.spark.ml

import org.apache.spark.sql.SparkSession

object CreditCardDataExploratory {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .master("local")
        .appName("CreditCardDataAnalysis")
        .getOrCreate()

    val inputDF = spark.read.option("header","true").csv("data/creditdata")
    val fraud = inputDF.filter {row =>
      val label = row.toSeq.last.asInstanceOf[String].toDouble
      label.equals(1.0)
    }.count

    println(s"Fraud number:$fraud, Total number:${inputDF.count}")
    val percentage = 1- fraud / inputDF.count.toDouble
    println(s"Fraud negative percentage:$percentage ")
  }
}
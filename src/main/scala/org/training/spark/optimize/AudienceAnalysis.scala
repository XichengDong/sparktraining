package org.training.spark.optimize

import org.apache.spark._

object AudienceAnalysis {

  lazy val nameIndexMap = {
    val nameIndexMap = scala.collection.mutable.HashMap.empty[String, Int]
    val basicNames = Seq("first_name", "last_name", "email", "company", "job", "street_address", "city",
      "state_abbr", "zipcode_plus4", "url", "phone_number", "user_agent", "user_name")
    nameIndexMap ++= basicNames zip (0 to 12)
    for(i <- 0 to 328) {
      nameIndexMap ++= Seq(("letter_" + i, i * 3 + 13), ("number_" + i,  i * 3 + 14), ("bool_" + i, i * 3 + 15))
    }
    nameIndexMap
  }
  def $(name: String): Int = nameIndexMap.getOrElse(name, -1)

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    /**
     * Step 1: Prepare RDDs
     */
    val DATA_PATH =  "/home/hadoop/input/txt"

    val peopleTxtRdd = sc.textFile(DATA_PATH)

    /**
     * s"SELECT city,state_abbr, count(*) FROM ${tableName} WHERE last_name NOT LIKE 'w%' " +
        s"AND email LIKE '%com%' AND letter_77 LIKE 'r' AND number_106 < 300 AND bool_143 = true " +
        s"AND letter_252 NOT LIKE 'o' AND number_311 > 400 GROUP BY city,state_abbr"
     */

    val resultRdd2 = peopleTxtRdd.map(_.split("\\|")).filter{ p =>
      !p($("last_name")).matches("^w") &&
          p($("email")).matches(".*com.*") &&
          p($("letter_77")).equals("r") &&
          p($("number_106")).toInt < 300 &&
          p($("bool_143")).toBoolean &&
          !p($("letter_252")).equals("o") &&
          p($("number_311")).toInt > 400
    }.map { p =>
      println("record:" + p($("city")))
      ((p($("city")), p($("state_abbr"))), 1)
    }.reduceByKey(_ + _, 2)

    resultRdd2.collect
  }
}
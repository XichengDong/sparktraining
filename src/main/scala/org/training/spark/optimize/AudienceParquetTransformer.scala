package org.training.spark.optimize

import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

object AudienceParquetTransformer {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
     * Step 1: Prepare RDDs
     */
    val DATA_PATH =  "/home/hadoop/input/txt"

    val peopleRdd = sc.textFile(DATA_PATH)


    val nameSeq = scala.collection.mutable.ArrayBuffer.empty[String]
    nameSeq ++= Seq("first_name", "last_name", "email", "company", "job", "street_address", "city",
      "state_abbr", "zipcode_plus4", "url", "phone_number", "user_agent", "user_name")
    for(i <- 0 to 328) {
      nameSeq ++= Seq("letter_" + i, "number_" + i, "bool_" + i)
    }

    val dataTypeSeq = scala.collection.mutable.ArrayBuffer.empty[DataType]
    for(i <- 0 to 12) {
      dataTypeSeq += StringType
    }
    for(i <- 0 to 328) {
      dataTypeSeq ++= Seq(StringType, ShortType, BooleanType)
    }

    val schema = StructType((nameSeq zip dataTypeSeq).
        map(nameType => StructField(nameType._1, nameType._2, true)))
    val peopleRowRdd = peopleRdd.map(_.split("\\|")).map{ p =>
      val row = scala.collection.mutable.ArrayBuffer.empty[scala.Any] ++ p
      for(i <- 13 to 999) {
        if(i % 3 == 2) row.update(i, row(i).asInstanceOf[String].toShort)
        else if(i % 3 == 0) row.update(i, row(i).asInstanceOf[String].toBoolean)
      }
      Row(row:_*)
    }
    val peopleDF = sqlContext.createDataFrame(peopleRowRdd, schema)
    peopleDF.write.format("parquet").mode("overwrite").save("/home/hadoop/input/parquet")

    val txtDF = peopleDF
    txtDF.registerTempTable("txtTable")

    val parquetDF = sqlContext.read.parquet("/home/hadoop/input/parquet")
    parquetDF.registerTempTable("parquetTable")
    /**
     * Step 2: Compare
     */

    // optimization: set spark.sql.shuffle.partitions=2
    val tableName = "parquetTable"
    val result = sqlContext.sql(s"SELECT city,state_abbr, COUNT(*) FROM ${tableName} WHERE last_name NOT LIKE 'w%' " +
        s"AND email LIKE '%com%' AND letter_77 LIKE 'r' AND number_106 < 300 AND bool_143 = true " +
        s"AND letter_252 NOT LIKE 'o' AND number_311 > 400 GROUP BY city,state_abbr").explain
  }
}
name := "spark-training"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "redis.clients" % "jedis" % "2.8.2",
  "com.alibaba" % "fastjson" % "1.2.14"
)
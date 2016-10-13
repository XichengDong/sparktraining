name := "spark-training"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "redis.clients" % "jedis" % "2.8.2",
  "com.alibaba" % "fastjson" % "1.2.14",
  "org.eclipse.jetty" % "jetty-server" % "9.2.5.v20141112",
  "org.eclipse.jetty" % "jetty-servlet" % "9.2.5.v20141112",
  "org.eclipse.jetty" % "jetty-util" % "9.2.5.v20141112",
  "org.glassfish.jersey.core" % "jersey-server" % "2.17",
  "org.glassfish.jersey.containers" % "jersey-container-servlet-core" % "2.17",
  "org.glassfish.jersey.containers" % "jersey-container-jetty-http" % "2.17",
  "org.glassfish.jersey.media" % "jersey-media-moxy" % "2.17",
  "javax.ws.rs" % "javax.ws.rs-api" % "2.0"
)
package org.training.spark.util

object KafkaRedisProperties {
  val REDIS_SERVER: String = "chinahadoop-1"
  val REDIS_PORT: Int = 6379

  val KAFKA_SERVER: String = "chinahadoop-1"
  val KAFKA_ADDR: String = KAFKA_SERVER + ":9092"
  val KAFKA_USER_TOPIC: String = "user_events"
  val KAFKA_RECO_TOPIC: String = "reco6"

}
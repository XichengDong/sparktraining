package org.training.spark.streaming

object KafkaRedisProperties {
  val REDIS_SERVER: String = "chinahadoop-1"
  val REDIS_PORT: Int = 6379

  val KAFKA_SERVER: String = "chinahadoop-1"
  val KAFKA_ADDR: String = KAFKA_SERVER + ":9092"
  val KAFKA_TOPICS: String = "user_events"
}
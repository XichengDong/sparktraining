package org.training.spark.reco.realtime

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.training.spark.proto.Spark.NewClickEvent
import org.training.spark.util.{RedisClient, KafkaRedisProperties}

object RealtimeRecommender {
  def main(args: Array[String]) {

    val Array(brokers, topics) = Array(KafkaRedisProperties.KAFKA_ADDR, KafkaRedisProperties.KAFKA_RECO_TOPIC)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RealtimeRecommender")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    messages.map(_._2).map{ event =>
      NewClickEvent.parseFrom(event)
    }.foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
          val jedis = RedisClient.pool.getResource
          partition.foreach { event =>
            println("NewClickEvent:" + event)
            val userId = event.getUserId
            val itemId = event.getItemId
            val key = "II:" + itemId
            val value = jedis.get(key)
            if (value != null) {
              jedis.set("RUI:" + userId, value)
              print("Finish recommendation to user:" + userId)
            }
          }
          // destroy jedis object, please notice pool.returnResource is deprecated
          jedis.close()
        }
    }
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

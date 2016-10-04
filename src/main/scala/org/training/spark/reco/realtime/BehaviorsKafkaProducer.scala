package org.training.spark.reco.realtime

import java.util.Properties
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.training.spark.proto.Spark.NewClickEvent
import org.training.spark.util.KafkaRedisProperties

object BehaviorsKafkaProducer {

  val newClickEvents= Seq(
    (1000000L, 123L),
    (1000001L, 400L),
    (1000002L, 500L),
    (1000003L, 278L),
    (1000004L, 681L)
  )

  def run(topic: String) {
    val props: Properties = new Properties
    props.put("metadata.broker.list", KafkaRedisProperties.KAFKA_ADDR)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    val conf: ProducerConfig = new ProducerConfig(props)
    var producer: Producer[Long, Array[Byte]] = null
    try {
      System.out.println("Producing messages")
      producer = new Producer[Long, Array[Byte]](conf)
      for (event <- newClickEvents) {
        val eventProto = NewClickEvent.newBuilder().setUserId(event._1).setItemId(event._2).build()
        producer.send(new KeyedMessage[Long, Array[Byte]](topic, event._1, eventProto.toByteArray))
        print("Sending messages:" + eventProto.toString)
      }
      println("Done sending messages")
    }
    catch {
      case ex: Exception => {
        println("Error while producing messages：" + ex)
      }
    } finally {
      if (producer != null) producer.close
    }
  }

  @throws(classOf[Exception])
  def main(args: Array[String]) {
    BehaviorsKafkaProducer.run(KafkaRedisProperties.KAFKA_RECO_TOPIC)
  }
}

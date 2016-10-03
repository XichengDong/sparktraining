package org.training.spark.streaming

import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

object RedisClient extends Serializable {
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new JedisPoolConfig(), KafkaRedisProperties.REDIS_SERVER,
    KafkaRedisProperties.REDIS_PORT, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
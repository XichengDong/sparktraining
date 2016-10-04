package org.training.spark.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisClient extends Serializable {
  private var MAX_IDLE: Int = 200
  private var TIMEOUT: Int = 10000
  private var TEST_ON_BORROW: Boolean = true

  lazy val config: JedisPoolConfig = {
    val config = new JedisPoolConfig
    config.setMaxIdle(MAX_IDLE)
    config.setTestOnBorrow(TEST_ON_BORROW)
    config
  }

  lazy val pool = new JedisPool(config, KafkaRedisProperties.REDIS_SERVER,
    KafkaRedisProperties.REDIS_PORT, TIMEOUT)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
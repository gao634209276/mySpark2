package redis

import redis.clients.jedis.{Jedis, Response}

/**
  */
object PipelineDemo {
  def main(args: Array[String]): Unit = {
    val redisHost = "localhost"
    val redisPassword = ""
    val redisPort = 8080
    //连接Redis，主要设置redisHost、redisPort
    val redisClient = new Jedis(redisHost, redisPort)
    // 如果有密码，需要进行密码验证
    redisClient.auth(redisPassword)


  }


  //没有使用Redis的pipeline，当数据较少的时候，可以用来使用
  def testNormal(redisClient: Jedis): Unit = {
    //读取Redis数据
    val keys = Array("key1", "key2", "key3", "key4")
    for (key <- keys) {
      println(redisClient.get(key))
    }
  }


  //使用pipeline批量读取数据之一（简化版）
  def testPipeline(redisClient: Jedis): Unit = {
    /**
      * 因为redis.clients.jedis.Jedis的pipelined下的get方法获取的是一个Response[String]类型的返回值，
      * 所以上面定义了一个临时变量Map[String, Response[String]]类型的tempRedisRes，
      * key是String类型，value是Response[String]类型，用于保存pp.get(key)的返回值。
      * 当for循环执行完之后，使用sync同步即可。这样便实现了Redis的Pipeline功能。
      */
    var tempRedisRes = Map[String, Response[String]]()
    val keys = Array("key1", "key2", "key3", "key4")
    val pp = redisClient.pipelined()
    for (key <- keys) {
      tempRedisRes ++= Map(key -> pp.get(key))
    }
    pp.sync()
  }


  //使用pipeline读取数据之二（强化版）
  def pipeline(redisClient: Jedis): Unit = {
    var tempRedisRes = Map[String, Response[String]]()
    val keys = Array("key1", "key2", "key3", "key4")
    var tryTimes = 2
    var flag = false
    // 为了防止连接Redis时的意外失败，我们需要设置一个尝试次数，确保数据一定程度上的正确性。
    while (tryTimes > 0 && !flag) {
      try {
        val pp = redisClient.pipelined()
        for (key <- keys) {
          tempRedisRes ++= Map(key -> pp.get(key))
        }
        pp.sync()
        flag = true
      } catch {
        case e: Exception => {
          flag = false
          println("Redis-Timeout" + e)
          tryTimes = tryTimes - 1
        }
      } finally {
        redisClient.disconnect()
      }
    }
  }
}

package stream.utils

import redis.clients.jedis.{Jedis, JedisPool}

object GetRedis {
    def getJedis(index:Int=0) ={
      //创建一个jedis连接池
      val pool: JedisPool = new JedisPool()
      //获取连接
      val jedis: Jedis = pool.getResource
      jedis.select(index)
      jedis
    }
}

package redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Iterator;
import java.util.Set;

public class RedisTest {
	public static void main(String[] args) {
		Jedis redis = RedisClient.getRedisClient().getJedis();
		//Pipeline pipeline = redis.pipelined();
		// todo
		redis.hset("test","a","b");
		Set<String> set = redis.keys("*");
		Iterator<String> it = set.iterator();
		while (it.hasNext()){
			it.next();
		}

		redis.hgetAll("test");

		//pipeline.syncAndReturnAll();
		RedisClient.getRedisClient().returnJedis(redis);
	}
}

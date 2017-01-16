package redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RedisPipeline {
	public static void main(String[] args) {
		Jedis redis = new Jedis("10.40.33.11", 6379, 400000);
		test(redis);
	}

	private static void test(Jedis redis){
		redis.select(1);
		redis.asking();
		redis.close();
		redis.disconnect();
	}
	private static void testhmset(Jedis redis) {
		Map<String, String> data = new HashMap<String, String>();
		redis.select(8);
		redis.flushDB();
		//hmset
		long start = System.currentTimeMillis();
		//直接hmset
		for (int i = 0; i < 10000; i++) {
			data.clear();
			data.put("k_" + i, "v_" + i);
			redis.hmset("key_" + i, data);
		}
		long end = System.currentTimeMillis();
		System.out.println("dbsize:[" + redis.dbSize() + "] .. ");
		System.out.println("hmset without pipeline used [" + (end - start) / 1000 + "] seconds ..");
		redis.select(8);
		redis.flushDB();
		redis.disconnect();
	}

	private static void hmget(Jedis redis) {
		//hmget
		Set<String> keys = redis.keys("*");
		//直接使用Jedis hgetall
		long start = System.currentTimeMillis();
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		for (String key : keys) {
			result.put(key, redis.hgetAll(key));
		}
		long end = System.currentTimeMillis();
		System.out.println("result size:[" + result.size() + "] ..");
		System.out.println("hgetAll without pipeline used [" + (end - start) / 1000 + "] seconds ..");
		redis.disconnect();
	}


	private static void pipelineHmset(Jedis redis) {
		//使用pipeline hmset
		Map<String, String> data = new HashMap<String, String>();
		Pipeline p = redis.pipelined();
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			data.clear();
			data.put("k_" + i, "v_" + i);
			p.hmset("key_" + i, data);
		}
		p.sync();
		long end = System.currentTimeMillis();
		System.out.println("dbsize:[" + redis.dbSize() + "] .. ");
		System.out.println("hmset with pipeline used [" + (end - start) / 1000 + "] seconds ..");
		redis.disconnect();
	}

	private static void pipelineHmget(Jedis redis) {
		Pipeline p = redis.pipelined();
		Set<String> keys = redis.keys("*");
		//使用pipeline hgetall
		Map<String, Response<Map<String, String>>> responses = new HashMap<String, Response<Map<String, String>>>(keys.size());
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		result.clear();
		long start = System.currentTimeMillis();
		for (String key : keys) {
			responses.put(key, p.hgetAll(key));
		}
		p.sync();
		for (String k : responses.keySet()) {
			result.put(k, responses.get(k).get());
		}
		long end = System.currentTimeMillis();
		System.out.println("result size:[" + result.size() + "] ..");
		System.out.println("hgetAll with pipeline used [" + (end - start) / 1000 + "] seconds ..");
		redis.disconnect();

	}

	private static void usePipeline(int count) {
		Jedis jr = null;
		try {
			jr = new Jedis("10.40.33.11", 6379);
			Pipeline pl = jr.pipelined();
			for (int i = 0; i < count; i++) {
				pl.incr("testKey2");
			}
			pl.sync();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (jr != null) {
				jr.disconnect();
			}
		}
	}
}
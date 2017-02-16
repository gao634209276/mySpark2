package redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.ArrayList;
import java.util.List;

public class RedisClient {
	private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);
	private JedisPool jedisPool;//非切片连接池
	private ShardedJedisPool shardedJedisPool;//切片连接池
	private static RedisClient redisClient;

	private RedisClient() {
		initialPool();
		initialShardedPool();
	}

	/**
	 * 单例RedisClient
	 *
	 * @return 获取RedisClient实例
	 */
	public static RedisClient getRedisClient() {
		if (redisClient == null) {
			synchronized (RedisClient.class) {
				if (redisClient == null) redisClient = new RedisClient();
			}
		}
		return redisClient;
	}

	/**
	 * 设置超时重试，从JedisPool中获取jedis对象
	 *
	 * @return 获取jedis对象
	 */
	public Jedis getJedis() {
		Jedis jedis = null;
		// 如果是网络超时则多试几次
		int timeoutCount = 0;
		while (true) {
			try {
				jedis = jedisPool.getResource();
				return jedis;
			} catch (Exception e) {
				// 底层原因是SocketTimeoutException，不过redis已经捕捉且抛出JedisConnectionException
				if (e instanceof JedisConnectionException) {
					timeoutCount++;
					logger.warn("getJedis timeoutCount={}", timeoutCount);
					if (timeoutCount > 3) {
						break;
					}
				} else {
					logger.warn("jedisInfo。NumActive=" + jedisPool.getNumActive());
					logger.error("getJedis error", e);
					break;
				}
			}
		}
		return null;
	}


	public void returnJedis(Jedis redis) {
		jedisPool.returnResource(redis);
	}

	public ShardedJedis getShardedJedis() {
		return shardedJedisPool.getResource();
	}

	/**
	 * 初始化非切片池
	 */
	private void initialPool() {
		// 池基本配置
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000l);
		config.setTestOnBorrow(false);

		jedisPool = new JedisPool(config, "10.40.33.11", 6379);
	}

	/**
	 * 初始化切片池
	 */
	private void initialShardedPool() {
		// 池基本配置
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000l);
		config.setTestOnBorrow(false);
		// slave链接
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		shards.add(new JedisShardInfo("10.40.33.11", 6379, "master"));
		// 构造池
		shardedJedisPool = new ShardedJedisPool(config, shards);
	}

	public void Close() {
		jedisPool.close();
		shardedJedisPool.close();
	}
}

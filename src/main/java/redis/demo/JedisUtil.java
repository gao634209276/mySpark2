package redis.demo;

import redis.clients.jedis.*;

import java.util.*;

/**
 */
public class JedisUtil {
	JedisPoolConfig config = null;
	JedisPool pool = null;
	private JedisSentinelPool sentinelpool;
	ShardedJedisPool shardPool = null;


	/**
	 * 获取化连接池配置
	 *
	 * @return JedisPoolConfig
	 */
	private JedisPoolConfig getPoolConfig() {
		if (config == null) {
			config = new JedisPoolConfig();
			//最大连接数
			config.setMaxTotal(Integer.valueOf(getResourceBundle().getString("redis.pool.maxTotal")));
			//最大空闲连接数
			config.setMaxIdle(Integer.valueOf(getResourceBundle().getString("redis.pool.maxIdle")));
			//获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
			config.setMaxWaitMillis(Long.valueOf(getResourceBundle().getString("redis.pool.maxWaitMillis")));
			//在获取连接的时候检查有效性, 默认false
			config.setTestOnBorrow(Boolean.valueOf(getResourceBundle().getString("redis.pool.testOnBorrow")));
			//在获取返回结果的时候检查有效性, 默认false
			config.setTestOnReturn(Boolean.valueOf(getResourceBundle().getString("redis.pool.testOnReturn")));
		}
		return config;
	}

	/**
	 * 普通连接池连接
	 * 这里展示的是普通的连接池方式链接redis的方案，跟普通的数据库连接池的操作方式类似；
	 * 初始化JedisPool
	 */
	private void initJedisPool() {
		if (pool == null) {
			//获取服务器IP地址
			String ipStr = getResourceBundle().getString("redis.ip");
			//获取服务器端口
			int portStr = Integer.valueOf(getResourceBundle().getString("redis.port"));
			//初始化连接池
			pool = new JedisPool(getPoolConfig(), ipStr, portStr);
		}
	}


	/**
	 * 该连接池用于应对Redis的Sentinel的主从切换机制，能够正确在服务器宕机导致服务器切换时得到正确的服务器连接，
	 * 当服务器采用该部署策略的时候推荐使用该连接池进行操作；
	 */
	private void initJedisSentinelPool() {
		if (sentinelpool == null) {
			//监听器列表
			Set<String> sentinels = new HashSet<String>();
			//监听器1
			sentinels.add(new HostAndPort("192.168.50.236", 26379).toString());
			//监听器2
			sentinels.add(new HostAndPort("192.168.50.237", 26379).toString());
			//实际使用的时候在properties里配置即可：redis.sentinel.hostandports=192.168 .50 .236:26379, 192.168 .50 .237:26379
			getResourceBundle().getString("redis.sentinel.hostandports");
			//mastername是服务器上的master的名字，在master服务器的sentinel.conf中配置:
			//[sentinel monitor server-1M  192.168.50.236 6379 2]
			//中间的server-1M即为这里的masterName
			String masterName = getResourceBundle().getString("redis.sentinel.masterName");
			//初始化连接池
			sentinelpool = new JedisSentinelPool(masterName, sentinels, getPoolConfig());
		}
	}

	/**
	 * ShardedJedisPool连接池分片连接
	 * 初始化ShardedJedisPool
	 * Redis在容灾处理方面可以通过服务器端配置Master-Slave模式来实现。
	 * 而在分布式集群方面目前只能通过客户端工具来实现一致性哈希分布存储，即key分片存储。
	 * Redis可能会在3.0版本支持服务器端的分布存储
	 */
	private void initShardedJedisPool() {

		if (shardPool == null) {

			// 创建多个redis共享服务
			ResourceBundle bundle = getResourceBundle();
			String redis1Ip = getResourceBundle().getString("redis1.ip");
			int redis1Port = Integer.valueOf(bundle.getString("redis.port"));
			JedisShardInfo jedisShardInfo1 = new JedisShardInfo(redis1Ip, redis1Port);
			String redis2Ip = getResourceBundle().getString("redis2.ip");
			int redis2Port = Integer.valueOf(bundle.getString("redis.port"));
			JedisShardInfo jedisShardInfo2 = new JedisShardInfo(redis2Ip, redis2Port);

			List<JedisShardInfo> serverlist = new LinkedList<JedisShardInfo>();
			serverlist.add(jedisShardInfo1);
			serverlist.add(jedisShardInfo2);

			// 初始化连接池
			shardPool = new ShardedJedisPool(getPoolConfig(), serverlist);
		}
	}

	/**
	 * 读写删除操作
	 *
	 * @return
	 */

/*	private void readWrite() {
		// 从池中获取一个Jedis对象
		Jedis jedis = sentinelpool.getSentinelpoolResource();
		String keys = "name";
		// 删除key-value对象，如果key不存在则忽略此操作
		jedis.del(keys);
		// 存数据
		jedis.set(keys, "snowolf");
		// 判断key是否存在，不存在返回false存在返回true
		jedis.exists(keys);
		// 取数据
		String value = jedis.get(keys);
		// 释放对象池（3.0将抛弃该方法）
		sentinelpool.returnSentinelpoolResource(jedis);
	}*/

	public ResourceBundle getResourceBundle() {
		return ResourceBundle.getBundle("redis");
	}
}

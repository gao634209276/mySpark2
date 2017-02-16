package spark;

import org.apache.spark.sql.*;
import redis.RedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Iterator;


public class Hive2Redis {

	public static void main(final String[] args) {
		if (args.length < 1) {
			System.out.println("spark-submit Hive2Redis hiveTable RedisZSet");
			args[0] = "test.words";
			args[1] = "test";
			//System.exit(1);
		}

		//Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		String warehouseLocation = "hdfs:///user/sinova/hive/warehouse";
		SparkSession spark = SparkSession.builder()
				.appName("Hive2Redis")
				.enableHiveSupport()
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.getOrCreate();
		String hiveTable = args[0];
		final String RedisZSet = args[1];

		Dataset<Row> tableDF = spark.table(hiveTable);
		Iterator<Row> it = tableDF.toLocalIterator();
		Long score = 0L;
		//Jedis jedis = RedisClient.getRedisClient().getJedis();
		Jedis jedis = new Jedis("10.40.33.11", 6379, 400000);//400秒超时设置
		Pipeline pl = jedis.pipelined();
		jedis.select(1);
		Row row;
		while (it.hasNext()) {
			row = it.next();
			score++;
			jedis.zadd(RedisZSet, score, row.getString(1));
			if (score % 10 == 0) {
				pl.sync();
			}
		}
		//RedisClient.getRedisClient().returnJedis(jedis);
		jedis.disconnect();
		jedis.close();
	}
}

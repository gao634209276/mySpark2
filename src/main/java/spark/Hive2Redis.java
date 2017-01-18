package spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import redis.RedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class Hive2Redis {

	public static void main(final String[] args) {
		if (args.length < 1) {
			System.out.println("spark-submit Hive2Redis hiveTable RedisZSet");
			System.exit(1);
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
		final LongAccumulator acc = new LongAccumulator();

		spark.table(hiveTable).foreachPartition(new ForeachPartitionFunction<Row>() {

			@Override
			public void call(java.util.Iterator<Row> t) throws Exception {

				Jedis jedis = RedisClient.getRedisClient().getJedis();
				jedis.select(1);
				Pipeline pl = jedis.pipelined();
				Row r;
				while (t.hasNext()) {
					r = t.next();
					acc.add(1);
					jedis.zadd(RedisZSet, r.getInt(0), String.valueOf(acc.value()));
				}
				pl.sync();
				RedisClient.getRedisClient().returnJedis(jedis);
			}
		});
	}
}

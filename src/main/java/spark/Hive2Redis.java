package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import redis.RedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

/**
 */
public class Hive2Redis {

	public static void main(String[] args) {
		//Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		String warehouseLocation = "hdfs:///user/hive/warehouse";
		SparkSession spark = SparkSession.builder()
				.appName("Hive2Redis")
				.enableHiveSupport()
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.getOrCreate();

		// 从java bean获取到一个Encoder
		//Encoder<Words> personEncoder = Encoders.bean(Words.class);
		Dataset<Row> ds = spark.table("test.words");
		//df.show();
		//df.select(col("word"), col("freq").plus(1)).show();
		ds.foreachPartition(new ForeachPartitionFunction<Row>() {
			@Override
			public void call(java.util.Iterator<Row> t) throws Exception {

				Jedis jedis = RedisClient.getRedisClient().getJedis();
				Pipeline pl = jedis.pipelined();
				Row r;
				while (t.hasNext()) {
					r = t.next();
					jedis.select(1);
					jedis.zadd("test", r.getInt(0), r.getString(1));
				}
				pl.sync();
				RedisClient.getRedisClient().returnJedis(jedis);
			}
		});
	}

}

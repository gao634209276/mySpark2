package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 */
public class Hive2RedisSparkContext {
	public static void main(final String[] args) {
		SparkConf conf = new SparkConf().setAppName("").setMaster("");
		JavaSparkContext sc = new JavaSparkContext(conf);

		Broadcast<int[]> broadcastVar = sc.broadcast(new int[]{1, 2, 3});
	}

}

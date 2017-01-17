package spark;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;

/**
 */
public class TestPairRDD {
	public static void main(String[] args) {
		//Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		String warehouseLocation = "hdfs:///user/sinova/hive/warehouse";
		SparkSession spark = SparkSession.builder()
				.appName("Hive2Redis")
				.enableHiveSupport()
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.getOrCreate();


				//.parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd"));
	}
}

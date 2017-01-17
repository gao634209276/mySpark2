package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

/**
 */
public class Hive2Redis {

	public static void main(String[] args) {
		//Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		String warehouseLocation = "hdfs:///user/sinova/hive/warehouse";
		SparkSession spark = SparkSession.builder()
				.appName("Hive2Redis")
				.enableHiveSupport()
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.getOrCreate();

		// 从java bean获取到一个Encoder
		Encoder<Words> personEncoder = Encoders.bean(Words.class);
		Dataset<Words> ds = spark.table("test.words").as(personEncoder);
		//df.show();
		//df.select(col("word"), col("freq").plus(1)).show();
		ds.foreach(new ForeachFunction<Words>() {
			public void call(Words words) throws Exception {
				System.out.println(words.toString());
			}
		});
	}

	/**
	 * java bean
	 */
	private static class Words {

		private int freq;
		private String word;

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public int getFreq() {
			return freq;
		}

		public void setFreq(int freq) {
			this.freq = freq;
		}

		@Override
		public String toString() {
			return "Words{" +
					"freq=" + freq +
					", word='" + word + '\'' +
					'}';
		}
	}
}

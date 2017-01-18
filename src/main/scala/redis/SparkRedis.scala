package redis

import org.apache.spark.sql.SparkSession

/**
	* Created by 小小科学家 on 2017/1/18.
	*/
object SparkRedis {

	def main(args: Array[String]) {
		val spark = SparkSession.builder()
			.enableHiveSupport()
			.appName("SparkRedis")
			.config("spark.sql.warehouse", "")
			.getOrCreate()

		//implicit org.apache.spark.
		val words = spark.table("test.words")
		words.foreachPartition(it => {
			while (it.hasNext) {
				val e = it.next()
				e.apply(1)
				e.apply(2)
			}
		})
	}
}

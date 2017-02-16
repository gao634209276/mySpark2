package spark

import org.apache.spark.sql.SparkSession

/**
	*/
object Spark2Broadcast {

	def main(args: Array[String]) {
		val spark = SparkSession.builder()
			.appName("Spark2Broadcast")
			.enableHiveSupport()
			.master("local")
			.getOrCreate()

	spark.sparkContext.broadcast("a")

	}
}

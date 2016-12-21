package hive

import org.apache.spark.sql.SparkSession

object HiveJoin {

	def main(args: Array[String]) {
		val spark = SparkSession.builder()
			.appName("HiveJoin")
			.master("yarn")
			.enableHiveSupport()
			.getOrCreate()

		joinWithSql(spark)
	}

	def joinWithDF(): Unit = {

	}

	def joinWithSql(spark: SparkSession): Unit = {
		import spark.implicits._
		import spark.sql
		//sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
		sql("USE hbase")
		sql("show tables").show()
		//val sqlDF = sql("SELECT * FROM tmp_twelve_flow_package_base")
		//sqlDF.createOrReplaceTempView("test")
		//sqlDF.show()
		//sqlDF.printSchema()

	}

	def joinWithDS(): Unit = {

	}
}

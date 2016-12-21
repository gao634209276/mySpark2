package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
	*/
object SparkSessionDB {
	def main(args: Array[String]) {
		val spark = SparkSession.builder()
			.appName("SparkSessionTest")
			.master("local")
			.config("spark.sql.warehouse.dir", "/")
			.config(new SparkConf().setJars(List("path/jar", "path/jar")))
			.config("spark.executor.memory", "4g")
			.config("url", "jdbc:oracle:thin:@host:1521:db")
			.config("driver", "oracle.jdbc.driver.OracleDriver")
			.config("user", "spark")
			.config("password", "spark")
			.config("", "")
			.getOrCreate()
		val df = spark.sql("select * form test")
		df.show()
		df.cache()
		df.persist()
	}

}

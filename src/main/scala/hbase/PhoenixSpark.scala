package hbase

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object PhoenixSpark {
	def main(args: Array[String]) {
		val spark = SparkSession
			.builder()
			.appName("PhoenixSpark")
			.master("yarn")
			.getOrCreate()

	}
}

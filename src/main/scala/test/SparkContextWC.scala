package test

import org.apache.spark.{SparkConf, SparkContext}

/**
	* Hello world!
	*/
object SparkContextWC {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("test").setMaster("local")

		val sc = new SparkContext(conf)
		sc.textFile("src/main/resources/people.txt")
			.flatMap(_.split(" "))
			.map((_, 1))
			.reduceByKey(_ + _)
			.collect()
			.foreach(println)
	}
}

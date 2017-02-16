package spark

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorTest {
	def main(args: Array[String]) {
		val conf = new SparkConf().setMaster("local").setAppName("AccumulatorDemo")
		val sc = new SparkContext(conf)
		val file = sc.textFile("data/text.txt")
		val blankLines = sc.accumulator(0)
		//val acc =new LongAccumulator
		val callSigns = file.flatMap(line => {
			if (line == "") {
				blankLines += 1
				//acc.add(1)
				//Console.println("Blank lines: " + blankLines.value)
				Console.println("Blank lines: " + blankLines.value)
			}
			line.split(" ")
		})

		//callSigns.collect().foreach(println)
	}
}

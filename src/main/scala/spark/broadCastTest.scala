package spark

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
	*/
object broadCastTest {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("broadCastTest").setMaster("local")
		val sc = new SparkContext(conf)


		val RDD = sc.parallelize(List(1, 2, 3))

		//broadcast
		val broadValue1 = sc.broadcast(2)
		val data1 = RDD.map(x => x * broadValue1.value)
		data1.foreach(x => println("broadcast value:" + x))


		//accumulator
		var accumulator = sc.accumulator(2)
		new  LongAccumulator();
		//错误
		val RDD2 = sc.parallelize(List(1, 1, 1)).map { x =>
			if (x < 3) {
				accumulator += 1
			}
			x * accumulator.value
		} //(x => x*accumulator.value)
		//此处还没有报错
		println(RDD2)
		//此处开始报错
		//RDD2.foreach(println)
		//  這里报错：Can't read accumulator value in task

		//這个操作没有报错
		RDD.foreach { x =>
			if (x < 3) {
				accumulator += 1
			}
		}
		println("accumulator is " + accumulator.value)
		// accumulator 说明了两点：
		//（1）： 累加器只有在执行Action的时候，才被更新
		//（2）：我们在task的时候不能读取它的值，只有驱动程序才可以读取它的值

		sc.stop()
	}
}


package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}

/**
	* SELECT ItemName,sum(Price * Quantity) AS OrderValue
	* FROM Items
	* JOIN Orders
	* ON Items.ItemID = Orders.ItemID
	* WHERE Orders.CustomerID > 'C002'
	* GROUP BY ItemName;
	*/
object HbaseJoin {
	def main(args: Array[String]) {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		//Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		val spark = SparkSession.builder()
			.appName("HbaseJoin")
			.master("yarn")
			.getOrCreate()
		//val conf = HBaseConfiguration.create()
		val conf = spark.sparkContext.hadoopConfiguration
		conf.addResource("../../conf/hbase-site.xml")
		//conf.set("hbase.zookeeper.quorum", "localhost")
		//conf.set("hbase.zookeeper.property.clientPort", "2181")
		val tablename = "ORDERS"
		conf.set(TableInputFormat.INPUT_TABLE, tablename)
		val sc = spark.sparkContext
		val ordersPairRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
			classOf[org.apache.hadoop.hbase.client.Result])
		val ordersRDD = ordersPairRDD.map(tuple => {
			val result = tuple._2
			(Bytes.toInt(result.getRow),
				Bytes.toString(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("CUSTOMERID"))),
				Bytes.toString(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("ITEMID"))),
				Bytes.toInt(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("QUANTITY"))),
				Bytes.toString(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("DATE"))))
		})
		ordersRDD.collect.foreach(println)


		conf.set(TableInputFormat.INPUT_TABLE, "ITEMS")
		val itemsPairRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
			classOf[org.apache.hadoop.hbase.client.Result])
		val itemsRDD = itemsPairRDD.map(tuple => {
			val result = tuple._2
			(Bytes.toString(result.getRow),
				Bytes.toString(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("ITEMNAME"))),
				Bytes.toDouble(result.getValue(Bytes.toBytes("0"), Bytes.toBytes("PRICE"))))
		})
		itemsRDD.collect.foreach(println)

		import spark.implicits._
		val ordersDF = ordersRDD.toDF("orderId", "customerId", "itemId", "quantity", "date")
		val itemsDF = itemsRDD.toDF("itemId", "itemName", "price")

		ordersDF.show()
		ordersDF.printSchema()
		itemsDF.show()
		itemsDF.printSchema()
		//`inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
		//val result = ordersDF.filter(ordersDF("customerId") > "C002").join(itemsDF, ordersDF("itemId") === itemsDF("itemId"), "left_outer")
		val join = ordersDF.filter($"customerId" > "C002")
			.join(itemsDF, Seq("itemId"), "left_outer")
		join.show()
		join.printSchema()

		// SELECT ItemName,sum(Price * Quantity) AS OrderValue
		//val multipliedDF = join.selectExpr("price*quantity")
		//val orderValue = join.withColumn("orderValue": String, join("price") * join("quantity"): Column)

		//val orderValue = join.select(join.col("price").multiply(join.col("quantity")).alias("orderValue"))
		//val result = orderValue.groupBy("itemName").sum("orderValue").select($"itemId", $"orderValue")

		//data.groupBy("gender").agg(count($"age"),max($"age").as("maxAge"), avg($"age").as("avgAge")).show
		//data.groupBy("gender").agg("age"->"count","age" -> "max", "age" -> "avg").show
		val orderValue1 = join.select($"itemName", ($"price" * $"quantity").alias("orderValue"))
		val result1 = orderValue1.groupBy("itemName").sum("orderValue")
		result1.show
		result1.printSchema()
	}
}

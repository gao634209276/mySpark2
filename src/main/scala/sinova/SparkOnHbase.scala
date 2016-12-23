package sinova

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkOnHbase {

	case class hbase(id: String, bz: String, detailid: String,
	                 creator: String, create_time: String,
	                 province_code: String, status: String,
	                 order_number: String,
	                 dataprovince: String, citycode: String, nettype: String,
	                 paymenttype: String, sequenceid: String,
	                 publishstatus: String, releasechannel: String,
	                 updateload: String, starttimes: String, endtimes: String
	                )

	def main(args: Array[String]) {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		val spark = SparkSession.builder()
				.appName("HbaseJoin")
				.enableHiveSupport()
				.master("yarn")
				.getOrCreate()
		val sc = spark.sparkContext

		/**
		  * +-------------------------------+-----------------+------------+------------+--------------+
		  * |          TABLE_NAME           |   COLUMN_NAME   | DATA_TYPE  | TYPE_NAME  | COLUMN_SIZE  |
		  * +-------------------------------+-----------------+------------+------------+--------------+
		  * | tmp_twelve_flow_package_base  | bz              | 1          | CHAR       | 4            |
		  * | tmp_twelve_flow_package_base  | id              | 12         | VARCHAR    | 50           |
		  * | tmp_twelve_flow_package_base  | detailid        | 12         | VARCHAR    | 50           |
		  * | tmp_twelve_flow_package_base  | creator         | 12         | VARCHAR    | 30           |
		  * | tmp_twelve_flow_package_base  | create_time     | 91         | DATE       | null         |
		  * | tmp_twelve_flow_package_base  | province_code   | -6         | TINYINT    | null         |
		  * | tmp_twelve_flow_package_base  | status          | -6         | TINYINT    | null         |
		  * | tmp_twelve_flow_package_base  | order_number    | 4          | INTEGER    | null         |
		  * | tmp_twelve_flow_package_base  | dataprovince    | -6         | TINYINT    | null         |
		  * | tmp_twelve_flow_package_base  | citycode        | -6         | TINYINT    | null         |
		  * | tmp_twelve_flow_package_base  | nettype         | -6         | TINYINT    | null         |
		  * | tmp_twelve_flow_package_base  | paymenttype     | -6         | TINYINT    | null         |
		  * | tmp_twelve_flow_package_base  | sequenceid      | 1          | CHAR       | 20           |
		  * | tmp_twelve_flow_package_base  | publishstatus   | -6         | TINYINT    | null         |
		  * | tmp_twelve_flow_package_base  | releasechannel  | 4          | INTEGER    | null         |
		  * | tmp_twelve_flow_package_base  | updateload      | 1          | CHAR       | 14           |
		  * | tmp_twelve_flow_package_base  | starttimes      | 91         | DATE       | null         |
		  * | tmp_twelve_flow_package_base  | endtimes        | 91         | DATE       | null         |
		  * +-------------------------------+-----------------+------------+------------+--------------+
		  */
		val hbaseRDD = getTable("tmp_twelve_flow_package_base", sc)
		//		val tableRDD = hbaseRDD.map(tuple => {
		//			val result = tuple._2
		//			(Bytes.toString(result.getRow), //id
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("bz"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("detailid"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("creator"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("create_time"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("province_code"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("status"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("order_number"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dataprovince"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("citycode"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("nettype"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("paymenttype"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sequenceid"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("publishstatus"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("releasechannel"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("updateload"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("starttimes"))),
		//				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("endtimes"))))
		//		})
		// RDD to DF
		import spark.implicits._

		// 通过case class转换
		val hbaseTableDF = hbaseRDD.map(tuple => {
			val result = tuple._2
			hbase(Bytes.toString(result.getRow), //id
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("bz"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("detailid"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("creator"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("create_time"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("province_code"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("status"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("order_number"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dataprovince"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("citycode"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("nettype"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("paymenttype"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sequenceid"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("publishstatus"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("releasechannel"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("updateload"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("starttimes"))),
				Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("endtimes"))))
		}).toDF()

		// 通过指定列名作为toDF参数
		//val colArray: Array[String] = Array("")
		//val hbaseDF = tableRDD.toDF(colArray: _*)
		hbaseTableDF.printSchema()
		//hbaseDF.show()
		val hbaseDF = hbaseTableDF.select($"DETAILID".alias("ID"), $"DATAPROVINCE", $"CITYCODE", $"NETTYPE", $"PAYMENTTYPE", $"PUBLISHSTATUS", $"RELEASECHANNEL", $"STARTTIMES", $"ENDTIMES")
		val hiveDF = spark.sql("select ID,PRODUCTNAME,PRODUCTCODE,PACKAGECODE,PACKAGENAME,FLOWKINDS,EFFICWAYS,TARIFFCODE from T_TWELVE_FLOW_PACKAGE_DETAIL")

		// DF ops
		val result = hbaseDF.join(hiveDF, Seq("ID"), "inner").distinct().select("ID",
			"DATAPROVINCE", "CITYCODE", "NETTYPE", "PAYMENTTYPE", "PUBLISHSTATUS", "RELEASECHANNEL",
			"STARTTIMES", "ENDTIMES", "PRODUCTNAME", "PRODUCTCODE", "PACKAGECODE", "PACKAGENAME",
			"FLOWKINDS", "EFFICWAYS", "TARIFFCODE")

		//result.select("ID", "DATAPROVINCE").show()
		createTable(sc, "result")

		sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "result")
		val job = Job.getInstance(sc.hadoopConfiguration)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

		val colArray: Array[String] = Array("ID",
			"DATAPROVINCE", "CITYCODE", "NETTYPE", "PAYMENTTYPE", "PUBLISHSTATUS", "RELEASECHANNEL",
			"STARTTIMES", "ENDTIMES", "PRODUCTNAME", "PRODUCTCODE", "PACKAGECODE", "PACKAGENAME",
			"FLOWKINDS", "EFFICWAYS", "TARIFFCODE")
		val rdd = result.rdd.map(row => {

			val put = new Put(Bytes.toBytes(row.getString(0)))
			for (i <- 1 until colArray.length) {
				put.add(Bytes.toBytes("info"), Bytes.toBytes(colArray(i)), Bytes.toBytes(row.getString(i)))
			}
			/*put.add(Bytes.toBytes("info"), Bytes.toBytes("DATAPROVINCE"), Bytes.toBytes(row.getString(1)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("CITYCODE"), Bytes.toBytes(row.getString(2)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("NETTYPE"), Bytes.toBytes(row.getString(3)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("PAYMENTTYPE"), Bytes.toBytes(row.getString(4)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("PUBLISHSTATUS"), Bytes.toBytes(row.getString(5)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("RELEASECHANNEL"), Bytes.toBytes(row.getString(6)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("STARTTIMES"), Bytes.toBytes(row.getString(7)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("ENDTIMES"), Bytes.toBytes(row.getString(8)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("PRODUCTNAME"), Bytes.toBytes(row.getString(9)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("PRODUCTCODE"), Bytes.toBytes(row.getString(10)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("PACKAGECODE"), Bytes.toBytes(row.getString(11)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("PACKAGENAME"), Bytes.toBytes(row.getString(12)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("FLOWKINDS"), Bytes.toBytes(row.getString(13)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("EFFICWAYS"), Bytes.toBytes(row.getString(14)))
			put.add(Bytes.toBytes("info"), Bytes.toBytes("TARIFFCODE"), Bytes.toBytes(row.getString(15)))*/
			(new ImmutableBytesWritable, put)
		})
		rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)


		// import org.apache.spark.sql.functions._
		// 其他见DS操作
		//hbaseDF.groupBy("gender").agg(max("age"), sum("expense"))
		// hbaseDF.filter("col1 > 10")
		// hbaseDF.filter($"col1" > 10)
		// hbaseDF.filter('col1 > 10)


		// DF to sql
		//hbaseDF.createOrReplaceTempView("hbaseTable")
		//hiveDF.createOrReplaceTempView("hiveTable")
		import spark.sql
		//sql("CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate'")
				val sqlDF = sql("select distinct'1004'," +
					"t1.DATAPROVINCE,t1.CITYCODE,t1.NETTYPE,t1.PAYMENTTYPE,t1.PUBLISHSTATUS,t1.RELEASECHANNEL," +
					"t1.STARTTIMES,t1.ENDTIMES," +
					"t2.PRODUCTNAME,t2.PRODUCTCODE,t2.PACKAGECODE,t2.PACKAGENAME,t2.FLOWKINDS,t2.EFFICWAYS,t2.TARIFFCODE" +
					" from hbaseTable t1" +
					" join hiveTable t2" +
					" on (t1.ID=t2.ID)")
				sqlDF.show()

		// RDD to DS,于DF类似
		// 通过case class转换
		/*		val hbaseDS = hbaseRDD.map(tuple => {
					val result = tuple._2
					hbase(Bytes.toString(result.getRow), //id
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("bz"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("detailid"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("creator"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("create_time"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("province_code"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("status"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("order_number"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dataprovince"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("citycode"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("nettype"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("paymenttype"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sequenceid"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("publishstatus"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("releasechannel"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("updateload"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("starttimes"))),
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("endtimes"))))
				}).toDS()*/


		// DS ops
		// import org.apache.spark.sql.functions._
		// hbaseDS.groupBy("gender").agg(max("age"), sum("expense"))
		// hbaseDS.groupBy("gender").agg(ImmutableMap.of("age", "max", "expense", "sum"))
		// hbaseDS.groupBy("gender").agg(Map("age" -> "max", "expense" -> "sum"))
		// hbaseDS.groupBy("gender").agg("age" -> "max", "expense" -> "sum")
	}

	def getTable(tablename: String, sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {

		//val conf = HBaseConfiguration.create()
		val conf = sc.hadoopConfiguration
		conf.set("hbase.zookeeper.quorum", "localhost")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set(TableInputFormat.INPUT_TABLE, tablename)
		sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
			classOf[org.apache.hadoop.hbase.client.Result])
	}

	def writeTable(df: DataFrame, tablename: String, sc: SparkContext): Unit = {
		sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "localhost")
		sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
		import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
		sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
		val job = Job.getInstance(sc.hadoopConfiguration)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

		val rdd = df.rdd.map { row => {
			/**
			  * 一个Put对象就是一行记录，在构造方法中指定主键
			  * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
			  * Put.add方法接收三个参数：列族，列名，数据
			  */
			val put = new Put(Bytes.toBytes(row.getInt(1)))
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(row.getString(2)))
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(row.getInt(3)))
			//转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
			(new ImmutableBytesWritable, put)
		}
		}
		//rdd.saveAsHadoopFile()
		rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
	}

	def createTable(sc: SparkContext, tableName: String): Unit = {
		val userTable = TableName.valueOf(tableName)
		val tableDescr = new HTableDescriptor(userTable)
		// create
		tableDescr.addFamily(new HColumnDescriptor("info".getBytes()))
		val admin = new HBaseAdmin(sc.hadoopConfiguration)
		if (admin.tableExists(userTable)) {
			admin.disableTable(userTable)
			admin.deleteTable(userTable)
		}
		admin.createTable(tableDescr)
	}
}

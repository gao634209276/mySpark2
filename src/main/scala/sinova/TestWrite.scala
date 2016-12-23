package sinova

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
	* Read form hive Write to Hbase
	*/
object TestWrite {


	def main(args: Array[String]) {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		val spark = SparkSession.builder()
			.master("yarn")
			.appName("HbaseRead")
			.config("hbase.zookeeper.quorum", "localhost")
			.config("hbase.zookeeper.property.clientPort", "2181")
			.enableHiveSupport()
			.getOrCreate()
		val sc = spark.sparkContext

		import spark.sql
		sql("show tables").show()
		val hiveDF = sql("select * from t_ods_3hall_intf where dt='20161121'").toDF()

		createTable(sc, "hiveTest")



		val colArray: Array[String] = Array("trans_id",
			"auto_id", "bss_id", "user_mobile", "province_id", "city_id", "net_id",
			"pay_id", "brand_id", "provider_key", "version", "interface_id", "interface_name",
			"interface_type", "result", "fail_reson", "response_code", "query_time",
			"change_type", "cur_package_id", "cur_package_name", "cur_package_type",
			"cur_product_id", "new_package_id", "new_package_name", "new_package_type",
			"new_product_type", "operate_type", "commit_type", "is_reback", "effect_type",
			"user_ip", "location", "wi", "sourceid", "access_sys", "json",
			"remark1", "remark2", "remark3", "remark4", "remark5",
			"bizend_time", "biz_hostip", "biz_process", "dt")

		// 1
		sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "hiveTest")
		val job = Job.getInstance(sc.hadoopConfiguration)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
		val hbaseRDD = hiveDF.rdd.map(row => {
			val put = new Put(Bytes.toBytes(row.getString(0)))
			for (i <- 1 until colArray.length) {
				put.add(Bytes.toBytes("info"), Bytes.toBytes(colArray(i)), Bytes.toBytes(row.getString(i)))
			}
			(new ImmutableBytesWritable, put)
		})
		hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)


		// 2
		// write
		//val myConf = spark.sparkContext.hadoopConfiguration
		//myConf.set("hbase.defaults.for.version.skip", "true")
		hiveDF.rdd.foreachPartition(iterator => {

			val myConf = HBaseConfiguration.create()
			myConf.set("hbase.zookeeper.quorum", "localhost")
			myConf.set("hbase.zookeeper.property.clientPort", "2181")
			myConf.set("hbase.defaults.for.version.skip", "true")
			// partition级别创建多个HTable客户端用于写操作，提高写数据的吞吐量
			//val myTable = new HTable(myConf, TableName.valueOf("hiveTest"))
			val conn = HConnectionManager.createConnection(myConf)
			val myTable = conn.getTable(TableName.valueOf("hiveTest"))
			//1:自动提交关闭，如果不关闭，每写一条数据都会进行提交，是导入数据较慢的做主要因素。
			myTable.setAutoFlush(false, false)
			//2:缓存大小，当缓存大于设置值时，hbase会自动提交。此处可自己尝试大小，一般对大数据量，设置为5M即可，本文设置为3M。
			myTable.setWriteBufferSize(5 * 1024 * 1024)

			iterator.foreach(row => {
				println(row(0) + ":::" + row(1))
				val p = new Put(Bytes.toBytes(row.getString(0)))
				for (i <- 1 until colArray.length) {
					p.add(Bytes.toBytes("info"), Bytes.toBytes(colArray(i)), Bytes.toBytes(row.getString(i)))
				}
				myTable.put(p)
			})
			//3:每一个分片结束后都进行flushCommits()，如果不执行，当hbase最后缓存小于上面设定值时，不会进行提交，导致数据丢失。
			//在close方法中自动调用myTable.flushCommits()]
			myTable.flushCommits()
			myTable.close()
			conn.close()
		})

		/*// 3
		sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "hiveTest")
		val hTable = new HTable(sc.hadoopConfiguration, TableName.valueOf("hiveTest"))
		val job = Job.getInstance(sc.hadoopConfiguration)
		job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setMapOutputValueClass(classOf[KeyValue])
		HFileOutputFormat2.configureIncrementalLoad(job, hTable)
		val hbaseRDD = hiveDF.rdd.map(row => {
			val put = new Put(Bytes.toBytes(row.getString(0)))
			for (i <- 1 until colArray.length) {
				put.add(Bytes.toBytes("info"), Bytes.toBytes(colArray(i)), Bytes.toBytes(row.getString(i)))
			}
			(new ImmutableBytesWritable, put)
		})
		val hdfsPath = "hdfs:///test.hfile"
		hbaseRDD.saveAsNewAPIHadoopFile(hdfsPath, classOf[ImmutableBytesWritable],
			classOf[KeyValue], classOf[HFileOutputFormat2], sc.hadoopConfiguration)
		//利用bulk load hfile
		val bulkLoader = new LoadIncrementalHFiles(sc.hadoopConfiguration)
		bulkLoader.doBulkLoad(new Path(hdfsPath), hTable)*/
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

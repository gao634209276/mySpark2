package sinova

import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
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

		sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "hiveTest")
		val job = Job.getInstance(sc.hadoopConfiguration)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

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
		val hbaseRDD = hiveDF.rdd.map(row => {
			val put = new Put(Bytes.toBytes(row.getString(0)))
			for (i <- 1 until colArray.length) {
				put.add(Bytes.toBytes("info"), Bytes.toBytes(colArray(i)), Bytes.toBytes(row.getString(i)))
			}
			(new ImmutableBytesWritable, put)
		})
		hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
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

package sinova

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import sinova.SparkOnHbase.hbase

/**
  * Created by 小小科学家 on 2016/12/21.
  */
object TestRead {
	def main(args: Array[String]) {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		val spark = SparkSession.builder()
				.master("yarn")
				.appName("HbaseRead")
				.config("hbase.zookeeper.quorum", "localhost")
				.config("hbase.zookeeper.property.clientPort", "2181")
				.getOrCreate()
		val sc = spark.sparkContext
		val conf = sc.hadoopConfiguration
		conf.set(TableInputFormat.INPUT_TABLE, "tmp_twelve_flow_package_base")
		import spark.implicits._
		val HbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
			classOf[org.apache.hadoop.hbase.client.Result])
				.map(tuple => {
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

		HbaseRDD.show()
	}
}

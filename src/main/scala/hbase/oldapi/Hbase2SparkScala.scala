package hbase.oldapi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object Hbase2SparkScala {
	def main(args: Array[String]) {
		//write()
		//read()
		testReadItem()
	}

	/**
		* 使用saveAsHadoopDataset写入数据
		*/
	def write(): Unit = {
		val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("yarn")
		val sc = new SparkContext(sparkConf)

		val conf = HBaseConfiguration.create()
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
		conf.set("hbase.zookeeper.quorum", "localhost")
		//设置zookeeper连接端口，默认2181
		conf.set("hbase.zookeeper.property.clientPort", "2181")

		val tablename = "account"

		//初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
		val jobConf = new JobConf(conf)
		import org.apache.hadoop.hbase.mapred.TableOutputFormat
		jobConf.setOutputFormat(classOf[TableOutputFormat])
		jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

		val indataRDD = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))


		val rdd = indataRDD.map(_.split(',')).map { arr => {
			/**
				* 一个Put对象就是一行记录，在构造方法中指定主键
				* 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
				* Put.add方法接收三个参数：列族，列名，数据
				*/
			val put = new Put(Bytes.toBytes(arr(0).toInt))
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
			//转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
			(new ImmutableBytesWritable, put)
		}
		}

		rdd.saveAsHadoopDataset(jobConf)

		sc.stop()
	}

	/**
		* 使用saveAsNewAPIHadoopDataset写入数据
		*/
	def writeNew(): Unit = {
		val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("yarn")
		val sc = new SparkContext(sparkConf)

		val tablename = "account"
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
		sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "localhost")
		//设置zookeeper连接端口，默认2181
		sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
		//初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
		import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
		sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

		val job = new Job(sc.hadoopConfiguration)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

		val indataRDD = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))
		val rdd = indataRDD.map(_.split(',')).map { arr => {
			/**
				* 一个Put对象就是一行记录，在构造方法中指定主键
				* 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
				* Put.add方法接收三个参数：列族，列名，数据
				*/
			val put = new Put(Bytes.toBytes(arr(0).toInt))
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
			//转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
			(new ImmutableBytesWritable, put)
		}
		}

		rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
	}

	/**
		* 从hbase读取数据转化成RDD
		*/
	def read(): Unit = {
		val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("yarn")
		val sc = new SparkContext(sparkConf)

		val tablename = "account"
		val conf = HBaseConfiguration.create()
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
		conf.set("hbase.zookeeper.quorum", "localhost")
		//设置zookeeper连接端口，默认2181
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set(TableInputFormat.INPUT_TABLE, tablename)

		// 如果表不存在则创建表
		val admin = new HBaseAdmin(conf)
		if (!admin.isTableAvailable(tablename)) {
			val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
			admin.createTable(tableDesc)
		}

		//读取数据并转化成rdd
		val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
			classOf[org.apache.hadoop.hbase.client.Result])

		val count = hBaseRDD.count()
		println(count)
		hBaseRDD.collect.foreach { case (_, result) => {
			//获取行键
			val key = Bytes.toInt(result.getRow)
			//通过列族和列名获取列
			val name = Bytes.toString(result.getValue("cf".getBytes, "name".getBytes))
			val age = Bytes.toInt(result.getValue("cf".getBytes, "age".getBytes))
			println("Row key:" + key + " Name:" + name + " Age:" + age)
		}
		}

		sc.stop()
		admin.close()
	}

	def testReadItem() = {
		val sparkConf = new SparkConf().setAppName("HBaseTest")
		val sc = new SparkContext(sparkConf)
		//定义 HBase 的配置
		val conf = HBaseConfiguration.create()
		// 表名
		conf.set(TableInputFormat.INPUT_TABLE, "ITEMS")
		//
		conf.set(TableInputFormat.SCAN_ROW_START, "I001")
		conf.set(TableInputFormat.SCAN_ROW_STOP, "I005")
		conf.set(TableInputFormat.SCAN_COLUMNS, "0:ITEMNAME")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set("hbase.zookeeper.quorum", "localhost")
		//指定输出格式和输出表名
		val jobConf = new JobConf(conf, this.getClass)
		//TableOutputFormat
		import org.apache.hadoop.hbase.mapred.TableOutputFormat
		jobConf.setOutputFormat(classOf[TableOutputFormat])
		jobConf.set(TableOutputFormat.OUTPUT_TABLE, "items")
		val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
			classOf[org.apache.hadoop.hbase.client.Result])

		hBaseRDD.count()
		// transform (ImmutableBytesWritable, Result) tuples into an RDD of Result’s
		val resultRDD = hBaseRDD.map(tuple => tuple._2)
		resultRDD.count()

		// transform into an RDD of (RowKey, ColumnValue)s  the RowKey has the time removed
		val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow).split(" ")(0), Bytes.toString(result.value).trim))
		keyValueRDD.collect().take(3).foreach(kv => println(kv))

		// group by rowkey , get statistics for column value
		//val keyStatsRDD = keyValueRDD.groupByKey().mapValues(list => StatCounter(list))
		//keyStatsRDD.collect().take(5).foreach(println)
	}

	def createHTable() {
		val userTable = TableName.valueOf("user")
		val tableDescr = new HTableDescriptor(userTable)
		// create
		tableDescr.addFamily(new HColumnDescriptor("basic".getBytes()))
		val hbaseConf: Configuration = HBaseConfiguration.create
		val admin = new HBaseAdmin(hbaseConf)
		if (admin.tableExists(userTable)) {
			admin.disableTable(userTable)
			admin.deleteTable(userTable)
		}
		admin.createTable(tableDescr)
	}

	def selectTable(): Unit = {
		val conf = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum", "")
		//val table = new HTab
	}
}

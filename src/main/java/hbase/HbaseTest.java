package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 */
public class HbaseTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("yarn").setAppName("sparkOnHbase");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Configuration hbaseConf = HBaseConfiguration.create();

		// scan 列族cf，列airName
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("cf"));
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("airName"));

		//

		try {
			// inputformat指定表名tableName
			hbaseConf.set(TableInputFormat.INPUT_TABLE, "tableName");
			//
			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			String scanToString = Base64.encodeBytes(proto.toByteArray());
			hbaseConf.set(TableInputFormat.SCAN, scanToString);

			// newAPIHadoopRDD读取
			JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(hbaseConf,
					TableInputFormat.class,
					ImmutableBytesWritable.class,
					Result.class);
			System.out.println(myRDD.count());
		} catch (IOException e) {
			e.printStackTrace();
		}
		//hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		//hbaseConf.set("hbase.zookeeper.quorum", "master");
	}

	// hbase操作必备
	private static Configuration getConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs:///hbase");
		// 使用eclipse时必须添加这个，否则无法定位
		conf.set("hbase.zookeeper.quorum", "hadoop1");
		return conf;
	}

	// 创建一张表
	public static void create(String tableName, String columnFamily)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (admin.tableExists(tableName)) {
			System.out.println("table exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
	}

	// 添加一条记录
	public static void put(String tableName, String row, String columnFamily,
						   String column, String data) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes
				.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "'," + columnFamily + ":" + column
				+ "','" + data + "'");
	}

	// 读取一条记录
	public static void get(String tableName, String row) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		System.out.println("Get: " + result);
	}

	// 显示所有数据
	public static void scan(String tableName) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("Scan: " + result);
		}
	}

	// 删除表
	public static void delete(String tableName) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Delete " + tableName + " 失败");
			}
		}
		System.out.println("Delete " + tableName + " 成功");
	}
}

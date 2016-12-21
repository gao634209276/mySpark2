package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 */
public class Hbase2Spark {
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
			conf.set(TableInputFormat.SCAN, scanToString);

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

}

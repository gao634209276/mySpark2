package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 */
public class HConnectionTest {
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		try {
			HTable table = new HTable(conf, TableName.valueOf("hiveTest"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void testGet() {
		Configuration conf = HBaseConfiguration.create();
		HConnection connection = null;
		try {
			connection = HConnectionManager.createConnection(conf);
			HTableInterface table = connection.getTable("myTable");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

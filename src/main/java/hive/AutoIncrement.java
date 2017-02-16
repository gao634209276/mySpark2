package hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFRowSequence.
 * add jar hiveUDF.jar
 * create temporary function auto_increment as 'sinova.hadoop.hive.udf.AutoIncrement';
 * select auto_increment(),telephone from xxx;
 */
@Description(name = "auto_increment",
		value = "_FUNC_() - Returns a generated row auto_increment number starting from 1",
		extended = "Example:\n"
				+ "> SELECT _FUNC_(),col FROM src;\n")
@UDFType(deterministic = false)
public class AutoIncrement extends UDF {
	private LongWritable result = new LongWritable();

	public AutoIncrement() {
		result.set(0);
	}

	public LongWritable evaluate() {
		//result++;
		result.set(result.get() + 1);
		return result;
	}
}
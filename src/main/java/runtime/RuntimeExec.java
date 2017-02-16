package runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 启动新的线程通过runtime.exec启动一个进程
 */
public class RuntimeExec {
	private static Log log = LogFactory.getLog(RuntimeExec.class);
	private String hiveTable;//hive表名
	private String redisZSet;//redis的zset名称


	private static String SPARK_SUBMIT = "spark-submit ";
	private static String MASTRE = " --master spark://10.40.33.12:7077 ";
	private static String JARS = " --jars /home/sinova/workSpace/commons-pool2-2.2.jar,/home/sinova/workSpace/jedis-2.6.2.jar ";
	private static String CLASS = " --class spark.Hive2Redis  /home/sinova/workSpace/mySpark2.jar";

	public RuntimeExec(String hiveTable, String redisZSet) {
		this.hiveTable = hiveTable;
		this.redisZSet = redisZSet;
	}
	public void submit() {
		try {
			Runtime rt = Runtime.getRuntime();
			log.info(" Exec Move hive Table " + hiveTable + "to Redis " + redisZSet);
			Process proc = rt.exec(SPARK_SUBMIT + MASTRE + JARS + CLASS + " " + hiveTable + " " + redisZSet);
			// any error message?
			StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "ERROR");
			// any output?
			StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "OUTPUT");
			// kick them off
			errorGobbler.start();
			outputGobbler.start();
			// 等待进程结束，exitVal表示
			int exitVal = proc.waitFor();
			log.info("ExitValue: " + exitVal);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
}

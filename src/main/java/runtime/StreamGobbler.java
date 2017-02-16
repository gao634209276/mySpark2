package runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * 启动新的线程接受子进程的标准输出和错误输出
 * 并用log4j打印
 */
public class StreamGobbler extends Thread {
	private static Log log = LogFactory.getLog(StreamGobbler.class);
	private InputStream is;
	private String type;

	StreamGobbler(InputStream is, String type) {
		this.is = is;
		this.type = type;
	}

	public void run() {
		try {
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			if (type.equals("ERROR")) {
				while ((line = br.readLine()) != null) {
					log.info("line");
				}
			} else if (type.equals("OUTPUT")) {
				while ((line = br.readLine()) != null) {
					log.error("line");
				}
			}
			br.close();
			isr.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
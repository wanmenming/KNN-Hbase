package cs62240.HBase;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class App {
	private static final Logger logger = LogManager.getLogger(Driver.class);

	public static void main(String[] args) {

		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new Driver(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}

	}
}

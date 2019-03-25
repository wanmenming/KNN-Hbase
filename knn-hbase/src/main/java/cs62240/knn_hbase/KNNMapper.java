/*
 * Mapper Class to for KNN algorithm
 */
package cs62240.knn_hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;

public class KNNMapper extends Mapper<Object, Text, Text, Text> {
	private static final Logger logger = LogManager.getLogger(KNNMapper.class);
	private final static Text one = new Text();
	private final static Text word = new Text();
	private final static ArrayList<PriorityQueue<Pair>> pqs = new ArrayList<>();
	private final static ArrayList<ArrayList<Long>> test_data = new ArrayList<>();
	private static int size;

	/* Commented out code for setup
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		size = Integer.parseInt(conf.get("size"));

		// Add the test data into data structure for next steps
		URI[] cacheFiles = context.getCacheFiles();
		FileSystem fs = FileSystem.get(conf);
		try {
			fs = FileSystem.get(new URI("s3://mr-k-nn"), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		// Add test data from files in HDFS
		for (int i = 0; i < cacheFiles.length; i++) {
			Path path = new Path(cacheFiles[i].toString());

			logger.info("The path of this file is " + path.toString());

			// Read line by line to get the test data
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = reader.readLine();
			while (line != null) {
				if (Character.isLetter(line.charAt(0))) {
					line = reader.readLine();
				}

				ArrayList<Long> tmp = new ArrayList<>();

				for (String element : line.split(",")) {

					// Convert the date separately
					if (element.contains("/")) {
						String[] date = element.split("/");
						tmp.add(Long.parseLong(date[0]) * 365 + Long.parseLong(date[1]) * 30 + Long.parseLong(date[2]));
					} else {
						if (element.length() == 0) {
							line = reader.readLine();
							continue;
						}
						tmp.add(Long.parseLong(element));
					}
				}
				line = reader.readLine();
				test_data.add(tmp);
			}
		}
	}
	*/

	private void getMatchingTrainingData(Context context, String partialKey) throws IOException {
		Configuration conf = context.getConfiguration();
		size = Integer.parseInt(conf.get("size"));
		
		String tableName = conf.get("tableName");
		
		if(tableName == null)
			tableName = "trainingdata";
		
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();
		// Instantiating HTable class
		HTable table = new HTable(config, tableName);
		// Instantiating the Scan class
		byte[] prefix=Bytes.toBytes(partialKey);
		Scan scan = new Scan(prefix);
		scan.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("DELAY_LABEL"));
		scan.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("FL_DATE"));
		scan.addColumn(Bytes.toBytes("delayinfo"), Bytes.toBytes("DEP_DELAY"));
		scan.addColumn(Bytes.toBytes("delayinfo"), Bytes.toBytes("ARR_DELAY"));
		scan.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("CRS_DEP_TIME"));
		scan.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("CRS_ARR_TIME"));
		scan.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("DEP_TIME"));
		scan.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("ARR_TIME"));
		scan.addColumn(Bytes.toBytes("location"), Bytes.toBytes("ORIGIN_AIRPORT_ID"));
		scan.addColumn(Bytes.toBytes("location"), Bytes.toBytes("DEST_AIRPORT_ID"));
		// Getting the scan result
		ResultScanner scanner = table.getScanner(scan);
		// Reading values from scan result
		for (Result r = scanner.next(); r != null; r = scanner.next()) {
			try {
				ArrayList<Long> tmp = new ArrayList<>();
				String flightDate = Bytes.toString(r.getValue(Bytes.toBytes("timetrack"), Bytes.toBytes("FL_DATE")));
				String[] date = flightDate.split("/");
				tmp.add(Long.parseLong(date[0]) * 365 + Long.parseLong(date[1]) * 30 + Long.parseLong(date[2]));
				
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("location"), Bytes.toBytes("ORIGIN_AIRPORT_ID")))));
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("location"), Bytes.toBytes("DEST_AIRPORT_ID")))));
				
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("timetrack"), Bytes.toBytes("CRS_DEP_TIME")))));
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("timetrack"), Bytes.toBytes("CRS_ARR_TIME")))));
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("timetrack"), Bytes.toBytes("DEP_TIME")))));
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("timetrack"), Bytes.toBytes("ARR_TIME")))));
				
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("delayinfo"), Bytes.toBytes("DEP_DELAY")))));
				tmp.add(Long.valueOf(Bytes.toString(r.getValue(Bytes.toBytes("delayinfo"), Bytes.toBytes("ARR_DELAY")))));
				
				for(int i = 0; i < tmp.size(); i++) {
					logger.info("*******************************" + tmp.get(i));
				}
				
				test_data.add(tmp);
				
			} catch (NumberFormatException e) {
				logger.error("*******************************"+e.getMessage());
			}
		}
		
		logger.info("**********************************"+test_data.size());

		// closing the scanner
		scanner.close();
	}

	@Override
	public void map(final Object key, final Text value, final Context context)
			throws IOException, InterruptedException {
		// Initialize StringTokenizeer with delimiter as ", " to split input csv file
		final StringTokenizer itr = new StringTokenizer(value.toString(), ", ");
		ArrayList<Long> trained_data = new ArrayList<>();

		// Traverse the number in the trained data and add it into the list
		while (itr.hasMoreTokens()) {
			String head = itr.nextToken().toString();

			// Skip the heading in the input file
			if (Character.isLetter(head.charAt(0))) {
				return;
			}

			// Extra process for date format
			if (head.contains("/")) {
				String[] date = head.split("/");
				trained_data
						.add(Long.parseLong(date[0]) * 365 + Long.parseLong(date[1]) * 30 + Long.parseLong(date[2]));
			} else {
				trained_data.add(Long.parseLong(head));
			}
		}

		// Skip incomplete train data
		if (trained_data.size() != 9) {
			return;
		}

		// Calculate the distance between this trained record with all other test data
		for (int data_index = 0; data_index < test_data.size(); data_index++) {
			int sum = 0;
			boolean delayed = false;

			// Add the priorityQueue into the pqs for the first time
			if (pqs.size() <= data_index) {
				pqs.add(new PriorityQueue<Pair>());
			}

			int element_index = 0;
			// Use Euclid distance to calculate the distance between the trained data
			for (; element_index < test_data.get(data_index).size(); element_index++) {
				sum += Math.pow(test_data.get(data_index).get(element_index) - trained_data.get(element_index), 2);
			}

			// Define the delay status of the trained data
			while (element_index < trained_data.size()) {
				if (element_index >= 7)
					delayed |= trained_data.get(element_index) > 0;
				element_index++;
			}

			// Add the pair into the priority queue and poll out data if it is out of size
			pqs.get(data_index).add(new Pair(sum, delayed));

			if (pqs.get(data_index).size() > size) {
				pqs.get(data_index).poll();
			}
		}

	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Emit the local winners in the priorityQueue for each test data
		for (int i = 0; i < test_data.size(); i++) {
			PriorityQueue<Pair> pq = pqs.get(i);
			while (!pq.isEmpty()) {
				Pair cur = pq.poll();
				word.set(test_data.get(i).toString());
				one.set(cur.getDist() + "," + cur.isDelayed());
				context.write(word, one);
			}
		}
	}
}

/*
 * Reducer Class to for KNN algorithm
 */
package cs62240.knn_hbase;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KNNReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger logger = LogManager.getLogger(KNNReducer.class);
	private final Text keys = new Text();
	private final Text value = new Text();
	private static int size;
	private static Configuration conf;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		size = Integer.parseInt(conf.get("size"));
	}
	
	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
		// Find k elements with the fewest distance for this test data
		PriorityQueue<Pair> pq = new PriorityQueue<Pair>(size);
		
		for (final Text val : values) {
			String[] elements = val.toString().split(",");
			pq.add(new Pair(Long.parseLong(elements[0]), elements[1].equals("true")?	true:false));
			
			// Poll out the element if the size of priorityQueue is out of size
			if (pq.size() > size) {
				pq.poll();
			}
		}
		
		// Emit the test data with its delay status
		keys.set(key.toString());
		value.set(checkDelay(pq));
		context.write(keys, value);
	}
	
	// Helper function to decide if this test data would be delayed or not
	public String checkDelay(PriorityQueue<Pair> pq){
		
		// Count the majority delay labels among k nearest neighbor of this test data
		int positive = 0;
		while (!pq.isEmpty()) {
			if (pq.poll().isDelayed()) {
				positive++;
			}
			else {
				positive--;
			}
		}
		
		logger.info("The majority label of this test data is " + positive);
		return positive > 0? "delayed":"normal";
	}

}
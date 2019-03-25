/*
 * Class to apply K-NN on flight dataset
 */

package knn;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KNN extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(KNN.class);
	
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("size", args[2]);
        
        // Set up the configuration of this job
        final Job knn = Job.getInstance(conf, "Apply the KNN algorithm on the input dataset");
        knn.setJarByClass(KNN.class);
		final Configuration initConf = knn.getConfiguration();
		initConf.set("mapreduce.output.textoutputformat.separator", "\t");

		// Add the test data into HDFS
//		FileSystem fs = FileSystem.get(new URI("s3://mr-k-nn"),initConf);
		FileSystem fs = FileSystem.get(initConf);
		
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[0]), false);
		while(fileStatusListIterator.hasNext()){
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			if (fileStatus.getPath().toString().endsWith("csv")) {
				knn.addCacheFile(fileStatus.getPath().toUri());
			}
		}
      
		// Run MR job to predict the delay label for each record in test_data file
		knn.setMapperClass(KNNMapper.class);
		knn.setReducerClass(KNNReducer.class);
		knn.setOutputKeyClass(Text.class);
		knn.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(knn, new Path(args[3]));
		FileOutputFormat.setOutputPath(knn, new Path(args[1]));
		knn.setMapOutputKeyClass(Text.class);
		knn.setMapOutputValueClass(Text.class);		
		
        return 	knn.waitForCompletion(true)?	1:0;
    }

    // Main function
    public static void main(String[] args) throws Exception {
    	if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new KNN(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
    }
}

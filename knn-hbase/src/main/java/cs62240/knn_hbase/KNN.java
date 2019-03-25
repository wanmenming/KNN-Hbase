/*
 * Class to apply K-NN on flight dataset
 */

package cs62240.knn_hbase;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.waiters.DescribeClusterFunction;

public class KNN extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(KNN.class);

	@Override
	public int run(String[] args) throws Exception {

		// create job for inserting data in HBase
		Configuration hbaseConf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(hbaseConf, args).getRemainingArgs();

		// comment this line to run this on local
		String dns = getPublicDNSName();

		Configuration hbaseConfig = HBaseConfiguration.addHbaseResources(hbaseConf);
		
		// comment these lines to run this on lcaol
		hbaseConfig.set("hbase.zookeeper.quorum", dns);
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		String tableName = args[4];
		if (!admin.isTableAvailable(tableName)) {

			HTableDescriptor td = new HTableDescriptor(tableName);
			HColumnDescriptor cd = new HColumnDescriptor("location");
			td.addFamily(cd);

			cd = new HColumnDescriptor("timetrack");
			td.addFamily(cd);

			cd = new HColumnDescriptor("delayinfo");
			td.addFamily(cd);

			admin.createTable(td);
		}
		admin.close();

		Job hbase = Job.getInstance(hbaseConf, "Hbase bulk data upload");
		Configuration hbaseJobConf = hbase.getConfiguration();

		hbaseConf.set("tableName", tableName);

		hbase.setJarByClass(KNN.class);
		hbase.setNumReduceTasks(0);

		hbase.setMapperClass(UploadMapper.class);
		hbase.setMapOutputKeyClass(ImmutableBytesWritable.class);
		hbase.setMapOutputValueClass(Put.class);

		HTable hTable = new HTable(hbaseConf, tableName);

		// Auto configure partitioner and reducer
		HFileOutputFormat2.configureIncrementalLoad(hbase, hTable);

		FileInputFormat.addInputPath(hbase, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(hbase, new Path(otherArgs[1] + 1));

		hbaseJobConf.set("mapreduce.output.textoutputformat.separator", ",");
		boolean success = hbase.waitForCompletion(true);

		if (success) {
			Configuration knnConf = this.getConf();
			knnConf.set("size", args[2]); // k value
			knnConf.set("tableName", args[4]);

			// Set up the configuration of this job
			final Job knn = Job.getInstance(knnConf, "Apply the KNN algorithm on the input dataset");
			knn.setJarByClass(KNN.class);
			final Configuration initConf = knn.getConfiguration();
			initConf.set("mapreduce.output.textoutputformat.separator", "\t");

//			// Add the test data into HDFS
//			FileSystem fs = FileSystem.get(new URI("s3://mr-k-nn"), initConf);
//			// FileSystem fs = FileSystem.get(initConf);
//
//			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[0]), false);
//			while (fileStatusListIterator.hasNext()) {
//				LocatedFileStatus fileStatus = fileStatusListIterator.next();
//
//				if (fileStatus.getPath().toString().endsWith("csv")) {
//					knn.addCacheFile(fileStatus.getPath().toUri());
//				}
//			}

			// Run MR job to predict the delay label for each record in test_data file
			knn.setMapperClass(KNNMapper.class);
			knn.setReducerClass(KNNReducer.class);
			knn.setOutputKeyClass(Text.class);
			knn.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(knn, new Path(args[0]));
			FileOutputFormat.setOutputPath(knn, new Path(args[1]));
			knn.setMapOutputKeyClass(Text.class);
			knn.setMapOutputValueClass(Text.class);
			return knn.waitForCompletion(true) ? 1 : 0;
		}
		return success ? 0 : 1;
	}

	/***
	 * Get the Cluster Id and the Public DNS name of the current AWS cluster
	 * 
	 * @return Public DNS Name of the current cluster
	 */
	private String getPublicDNSName() {
		String publicDNSName = "";

		// Set AWS access Key and secret key and get the EMR Client
		AmazonElasticMapReduce emrClient = new AmazonElasticMapReduceClient(new BasicAWSCredentials("AKIAJMMSU4XCYC7DJIJQ", "+vQOQxivPnkxzdnz8MY2Z0rK8RYEKUgaXDM5l6Fx"));
		Region euWest1 = Region.getRegion(Regions.US_EAST_1);
		emrClient.setRegion(euWest1);
		ListClustersRequest req = new ListClustersRequest().withClusterStates(ClusterState.RUNNING);

		ListClustersResult res = emrClient.listClusters(req);

		for (int i = 0; i < res.getClusters().size(); i++) {
			String clusterId = res.getClusters().get(i).getId();
			DescribeClusterFunction fun = new DescribeClusterFunction(emrClient);
			DescribeClusterResult res1 = fun.apply(new DescribeClusterRequest().withClusterId(clusterId));
			publicDNSName = res1.getCluster().getMasterPublicDnsName();

			logger.info(">>> Cluster: " + clusterId);
			logger.info(">>> publicDNSName: " + publicDNSName);
		}
		return publicDNSName;
	}

	// Main function
	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			throw new Error("Five arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new KNN(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}

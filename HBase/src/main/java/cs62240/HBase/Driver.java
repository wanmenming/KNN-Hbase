package cs62240.HBase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.waiters.DescribeClusterFunction;

/**
 * HBase bulk import Data preparation MapReduce job driver
 */
public class Driver extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Driver.class);

	@Override
	public int run(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String dns = getPublicDNSName();

//		Configuration config = HBaseConfiguration.create();
//		List<String> nodes = new ArrayList<>();
//		nodes.add(dns);
//		Cluster cluster = new Cluster(nodes);
//		Client client = new Client(cluster);
//		RemoteAdmin admin = new RemoteAdmin(client, config);

		// Load hbase-site.xml
		Configuration hbaseConfig = HBaseConfiguration.addHbaseResources(conf);

//		hbaseConfig.addResource(new Path("/home/hadoop/hbase/conf/hbase-site.xml"));
		// hbaseConfig.addResource(new
		// Path("/usr/local/hadoop-2.9.1/etc/hadoop/core-site.xml"));

		hbaseConfig.set("hbase.zookeeper.quorum", dns);
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		String tableName = "test1";
		if (!admin.isTableAvailable(tableName)) {

			HTableDescriptor td = new HTableDescriptor(tableName);
			HColumnDescriptor cd = new HColumnDescriptor("flight");
			td.addFamily(cd);

			cd = new HColumnDescriptor("time");
			td.addFamily(cd);

			cd = new HColumnDescriptor("flags");
			td.addFamily(cd);

			admin.createTable(td);
		}

		Job job = Job.getInstance(conf, "Hbase bulk data upload");
		Configuration jobConf = job.getConfiguration();

		job.setJarByClass(Driver.class);
		job.setNumReduceTasks(0);

		job.setMapperClass(UploadMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		HTable hTable = new HTable(conf, "test1");

		// Auto configure partitioner and reducer
		HFileOutputFormat2.configureIncrementalLoad(job, hTable);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		int success = job.waitForCompletion(true) ? 0 : 1;
		
		readData(conf);
		
		return success;
	}

//	private String getPublicDNSName() {
//		AWSCredentials credentials = null;
//		String publicDNSName = "";
//
//		try {
//			ClassLoader cla = Thread.currentThread().getContextClassLoader();
//			File file = new File(cla.getResource("awsconfig.properties").getFile());
//			credentials = new PropertiesCredentials(file);
//		} catch (IOException e1) {
//			System.out.println("Credentials were not properly entered into AwsCredentials.properties.");
//			System.out.println(e1.getMessage());
//			System.exit(-1);
//		}
//		AmazonElasticMapReduce emrClient = new AmazonElasticMapReduceClient(credentials);
//		Region euWest1 = Region.getRegion(Regions.US_EAST_1);
//		emrClient.setRegion(euWest1);
//		ListClustersRequest req = new ListClustersRequest().withClusterStates(ClusterState.RUNNING);
//
//		ListClustersResult res = emrClient.listClusters(req);
//
//		for (int i = 0; i < res.getClusters().size(); i++) {
//			String clusterId = res.getClusters().get(i).getId();
//			DescribeClusterFunction fun = new DescribeClusterFunction(emrClient);
//			DescribeClusterResult res1 = fun.apply(new DescribeClusterRequest().withClusterId(clusterId));
//			publicDNSName = res1.getCluster().getMasterPublicDnsName();
//
//			logger.info(">>> Cluster: " + clusterId);
//			logger.info(">>> publicDNSName: " + publicDNSName);
//		}
//		return publicDNSName;
//	}
	
	private String getPublicDNSName() {
		String publicDNSName = "";

		AmazonElasticMapReduce emrClient = new AmazonElasticMapReduceClient(new BasicAWSCredentials("AKIAJS3PO7KTXO6P63MA", "jD0kTnODOc1UDDsu6CtnFemALgpbbm7RKj7oAIVu"));
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

	private void readData(Configuration conf) throws IOException {
//		String dns = getPublicDNSName();
//		Configuration config = HBaseConfiguration.create();
//		List<String> nodes = new ArrayList<>();
//		nodes.add(dns);
//		Cluster cluster = new Cluster(nodes);
//		Client client = new Client(cluster);

		// Instantiating HTable class
//		RemoteHTable hTable = new RemoteHTable(client, config, "test1");
		HTable hTable = new HTable(conf, "test1");
		Get g = new Get(Bytes.toBytes("N26232"));

		org.apache.hadoop.hbase.client.Result r = hTable.get(g);
		logger.info("*********** 1 = " + Bytes.toString(r.getValue(Bytes.toBytes("flight"), Bytes.toBytes("flightdate"))));
		logger.info("*********** 3 = " + Bytes.toString(r.getValue(Bytes.toBytes("flight"), Bytes.toBytes("flightnumber"))));
		logger.info("*********** 4 = " + Bytes.toString(r.getValue(Bytes.toBytes("flight"), Bytes.toBytes("origin"))));
		logger.info("*********** 5 = " + Bytes.toString(r.getValue(Bytes.toBytes("flight"), Bytes.toBytes("dest"))));
		logger.info("*********** 6 = " + Bytes.toString(r.getValue(Bytes.toBytes("time"), Bytes.toBytes("depdelay"))));
		logger.info("*********** 7 = " + Bytes.toString(r.getValue(Bytes.toBytes("time"), Bytes.toBytes("arrdelay"))));
		logger.info("*********** 8 = " + Bytes.toString(r.getValue(Bytes.toBytes("flags"), Bytes.toBytes("cancelled"))));
		logger.info("*********** 9 = " + Bytes.toString(r.getValue(Bytes.toBytes("flags"), Bytes.toBytes("diverted"))));
		logger.info("*********** 10 = " + Bytes.toString(r.getValue(Bytes.toBytes("flight"), Bytes.toBytes("dist"))));
		hTable.close();

	}
}
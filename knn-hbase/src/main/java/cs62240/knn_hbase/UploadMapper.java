package cs62240.knn_hbase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
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

/**
 * HBase bulk import
 */
public class UploadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private static final Logger logger = LogManager.getLogger(UploadMapper.class);
//	RemoteHTable hTable;
	HTable hTable;

	@Override
	protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {

		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		
		String tableName = conf.get("tableName");

//		String dns = getPublicDNSName();
		Configuration config = HBaseConfiguration.create();
//		List<String> nodes = new ArrayList<>();
//		nodes.add(dns);
//		Cluster cluster = new Cluster(nodes);
//		Client client = new Client(cluster);

		// Instantiating HTable class
//		hTable = new RemoteHTable(client, config, "test1");
		
		if(tableName == null)
			tableName = "trainingdata";
		
		hTable = new HTable(config, tableName);

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (Character.isLetter(value.toString().charAt(0))) {
			return;
		}
		
		Date date = new Date();
		
		String[] fields = value.toString().split(",");
		
		int size = fields.length;

		if (size > 0) {
			String rowkey = fields[0] + fields[1] + date.getTime(); // composite row key => FL_DATE + ORIGIN_AIRPORT_ID + Current Time in milliseconds
			byte[] bKey = Bytes.toBytes(rowkey);
			Put put = new Put(bKey);
			if (size == 10) {
				put.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("DELAY_LABEL"), Bytes.toBytes(fields[9]));
				put.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("FL_DATE"), Bytes.toBytes(fields[0]));
				put.addColumn(Bytes.toBytes("location"), Bytes.toBytes("ORIGIN_AIRPORT_ID"), Bytes.toBytes(fields[1]));
				put.addColumn(Bytes.toBytes("location"), Bytes.toBytes("DEST_AIRPORT_ID"), Bytes.toBytes(fields[2]));
				put.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("CRS_DEP_TIME"), Bytes.toBytes(fields[3]));
				put.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("CRS_ARR_TIME"), Bytes.toBytes(fields[4]));
				put.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("DEP_TIME"), Bytes.toBytes(fields[5]));
				put.addColumn(Bytes.toBytes("timetrack"), Bytes.toBytes("ARR_TIME"), Bytes.toBytes(fields[6]));
				put.addColumn(Bytes.toBytes("delayinfo"), Bytes.toBytes("DEP_DELAY"), Bytes.toBytes(fields[7]));
				put.addColumn(Bytes.toBytes("delayinfo"), Bytes.toBytes("ARR_DELAY"), Bytes.toBytes(fields[8]));
				ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
				//		context.write(ibKey, put);
				hTable.put(put);
			}
		}

	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		if (hTable != null) {
			hTable.close();
		}
	}

//	private String getPublicDNSName() {
//		String publicDNSName = "";
//
//		AmazonElasticMapReduce emrClient = new AmazonElasticMapReduceClient(new BasicAWSCredentials("AKIAJS3PO7KTXO6P63MA", "jD0kTnODOc1UDDsu6CtnFemALgpbbm7RKj7oAIVu"));
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
}
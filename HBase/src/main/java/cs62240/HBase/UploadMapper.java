package cs62240.HBase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
		// Instantiating Configuration class

//		String dns = getPublicDNSName();
		Configuration config = HBaseConfiguration.create();
//		List<String> nodes = new ArrayList<>();
//		nodes.add(dns);
//		Cluster cluster = new Cluster(nodes);
//		Client client = new Client(cluster);

		// Instantiating HTable class
//		hTable = new RemoteHTable(client, config, "test1");
		hTable = new HTable(config, "test1");

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");

		byte[] bKey = Bytes.toBytes(fields[1]);
		Put put = new Put(bKey);

		put.addColumn(Bytes.toBytes("flight"), Bytes.toBytes("flightdate"), Bytes.toBytes(fields[0]));
		put.addColumn(Bytes.toBytes("flight"), Bytes.toBytes("flightnumber"), Bytes.toBytes(fields[2]));
		put.addColumn(Bytes.toBytes("flight"), Bytes.toBytes("origin"), Bytes.toBytes(fields[3]));
		put.addColumn(Bytes.toBytes("flight"), Bytes.toBytes("dest"), Bytes.toBytes(fields[4]));
		put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("depdelay"), Bytes.toBytes(fields[5]));
		put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("arrdelay"), Bytes.toBytes(fields[6]));
		put.addColumn(Bytes.toBytes("flags"), Bytes.toBytes("cancelled"), Bytes.toBytes(fields[7]));
		put.addColumn(Bytes.toBytes("flags"), Bytes.toBytes("diverted"), Bytes.toBytes(fields[8]));
		put.addColumn(Bytes.toBytes("flight"), Bytes.toBytes("dist"), Bytes.toBytes(fields[9]));

		ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
//		context.write(ibKey, put);
		hTable.put(put);

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
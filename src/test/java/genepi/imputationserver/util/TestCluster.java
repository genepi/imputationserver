package genepi.imputationserver.util;

import genepi.hadoop.HdfsUtil;
import genepi.io.FileUtil;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

public class TestCluster {

	private static TestCluster instance;

	private static String WORKING_DIRECTORY = "test-cluster";

	private MiniDFSCluster cluster;

	private FileSystem fs;

	private Configuration conf;

	private TestCluster() {

	}

	public static TestCluster getInstance() {
		if (instance == null) {
			instance = new TestCluster();
		}
		return instance;
	}

	public void start() throws IOException {

		File testCluster = new File(WORKING_DIRECTORY);
		if (testCluster.exists()) {
			FileUtil.deleteDirectory(testCluster);
		}
		testCluster.mkdirs();
		
		File testClusterData = new File(WORKING_DIRECTORY + "/data");
		File testClusterLog = new File(WORKING_DIRECTORY + "/logs");

		
		if (cluster == null) {

			conf = new HdfsConfiguration();		
			conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
					testClusterData.getAbsolutePath());
			cluster = new MiniDFSCluster.Builder(conf).build();
			fs = cluster.getFileSystem();

			// set mincluster as default config
			HdfsUtil.setDefaultConfiguration(conf);
			System.setProperty("hadoop.log.dir", testClusterLog.getAbsolutePath());
			MiniMRCluster mrCluster = new MiniMRCluster(1, fs.getUri()
					.toString(), 1, null, null, new JobConf(conf));
			JobConf mrClusterConf = mrCluster.createJobConf();
			HdfsUtil.setDefaultConfiguration(new Configuration(mrClusterConf));

			System.out.println("------");

			JobClient client = new JobClient(mrClusterConf);
			ClusterStatus status = client.getClusterStatus(true);
			System.out.println(status.getActiveTrackerNames());
		}
	}
	
	public void stop(){
		File testCluster = new File(WORKING_DIRECTORY);
		if (testCluster.exists()) {
			FileUtil.deleteDirectory(testCluster);
		}
	}

}

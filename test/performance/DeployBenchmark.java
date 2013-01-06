package performance;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.rapportive.storm.spout.AMQPSpout;

import moa.storm.topology.BenchmarkingTopology;
import moa.storm.topology.StormClusterTopology;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.builtin.Debug;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.cluster.StormClusterState;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class DeployBenchmark {
	
	
	private static String STORM_HOME="/home/bsp/storm-0.8.2-wip8";
	static byte buffer[] = new byte[1024];
	
	
	
	public static void main(String[] args) throws Throwable
	{
		Properties prp = new Properties();
		prp.load(DeployBenchmark.class.getResourceAsStream("/ml_storm_cluster.properties"));
		
		String classifier = "";
		for (int i = 3 ; i < args.length ; i ++ )
		{
			classifier += " " + args[i];
		}
		classifier = classifier.trim();
		
		String topologyName = prp.getProperty("storm.topology_name");
		
        Map config = Utils.readStormConfig();
		Config conf = new Config();
		conf.putAll(config);
		BenchmarkingTopology storm = new BenchmarkingTopology("/ml_storm_cluster.properties");
		storm.setClassiferOption(classifier);
		
		conf.put(AMQPSpout.CONFIG_PREFETCH_COUNT, 10);
		
		HashMap  tridentconfig = new HashMap();
		tridentconfig.put(storm.BAGGING_ENSEMBLE_SIZE, args[0]);
		tridentconfig.put("learning.parallelism", args[1]);
		tridentconfig.put("evaluation.parallelism",args[2]);
	//	conf.setMaxTaskParallelism(conf, 4);
		if ("true".equals(System.getProperty("localmode")))
		{
			LocalCluster cls = new LocalCluster();
			LocalDRPC local = new LocalDRPC();
			tridentconfig.put("rpc", local);
			cls.submitTopology(topologyName,  conf,storm.createOzaBag(tridentconfig).build());
		}
		else
		{
			conf.setNumWorkers(conf, 28);
			StormSubmitter.submitTopology(topologyName, conf, storm.createOzaBag(tridentconfig).build());
			
		}
		
	}

}

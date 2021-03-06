package performance;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import moa.trident.topology.BenchmarkingTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

import com.rapportive.storm.spout.AMQPSpout;

public class DeployBenchmark {
	
	
	private static String STORM_HOME="/home/bsp/storm-0.8.2-wip8";
	static byte buffer[] = new byte[1024];
	
	
	
	public static void main(String[] args) throws Throwable
	{
		Properties prp = new Properties();
		prp.load(DeployBenchmark.class.getResourceAsStream("/ml_storm_cluster.properties"));
		
		String classifier = "";
		for (int i = 5 ; i < args.length ; i ++ )
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
		//conf.put("worker.childopts", "-Xmx256m");
		//conf.put("topology.worker.childopts", "-Xmx256m");
		conf.put(AMQPSpout.CONFIG_PREFETCH_COUNT, 10);
		
		HashMap  bagging_config = new HashMap();
		bagging_config.put(storm.BAGGING_ENSEMBLE_SIZE, args[0]);
		bagging_config.put("learning.parallelism",args[2]);
		bagging_config.put("evaluation_stream.parallelism",args[3]);
		bagging_config.put("evaluation_merge.parallelism",args[4]);
		
		conf.put("supervisor.worker.start.timeout.secs", 1200);
		conf.setNumWorkers(Integer.parseInt(args[1]));
		//conf.setDebug(true);
		
	//	conf.setMaxTaskParallelism(conf, 4);
		if ("true".equals(System.getProperty("localmode")))
		{
			
			StormTopology learner = storm.createOzaBagLearner(bagging_config).build();
			TopologyPrinter printer = new TopologyPrinter();
			printer.print(learner);
			LocalCluster cls = new LocalCluster();
		
			conf.put("topology.spout.max.batch.size", 2);
			cls.submitTopology(topologyName+"_learner", conf, learner);
			///cls.submitTopology(topologyName+"_evaluator",  conf,topology);
			
			//Thread.sleep(20 * 1000);
			//System.exit(0);
		}
		else
		{
			StormSubmitter.submitTopology(topologyName+"_learner", conf, storm.createOzaBagLearner(bagging_config).build());
				
		   
		}
		
	}

}

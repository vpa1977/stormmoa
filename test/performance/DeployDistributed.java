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

public class DeployDistributed {
	
	
	private static String STORM_HOME="/home/bsp/storm-0.8.2-wip8";
	static byte buffer[] = new byte[1024];
	
	
	
	public static void main(String[] args) throws Throwable
	{
		Properties prp = new Properties();
		prp.load(DeployDistributed.class.getResourceAsStream("/ml_storm_cluster.properties"));
		
		String classifier = "";
		for (int i = 3 ; i < args.length ; i ++ )
		{
			classifier += " " + args[i];
		}
		classifier = classifier.trim();

		
		//ZipOutputStream zos = new ZipOutputStream(new FileOutputStream("topology.jar"));
		//populate(zos, new File("bin"), new File("bin").getAbsolutePath());
		//zos.close();
		//System.setProperty("storm.home", prp.getProperty("storm.home"));
		//System.setProperty("storm.jar", prp.getProperty("storm.jar"));
		
		String topologyName = prp.getProperty("storm.topology_name");
		
        Map config = Utils.readStormConfig();
		Config conf = new Config();
		conf.putAll(config);

		try {
			NimbusClient client = NimbusClient.getConfiguredClient(conf);
			client.getClient().killTopology(topologyName);
		}
		catch (Throwable t){}
	    
		StormClusterTopology storm = new StormClusterTopology("/ml_storm_cluster.properties");
		storm.setClassiferOption(classifier);
		
		conf.put(AMQPSpout.CONFIG_PREFETCH_COUNT, 10000);
		//conf.put("topology.max.spout.pending",  10000);
		HashMap  tridentconfig = new HashMap();
		tridentconfig.put(storm.BAGGING_ENSEMBLE_SIZE, args[0]);
		tridentconfig.put("learning.parallelism", args[1]);
		tridentconfig.put("evaluation.parallelism",args[2]);
	//	conf.setMaxTaskParallelism(conf, 4);
		
		conf.setNumWorkers(conf, 28);
		StormSubmitter.submitTopology(topologyName, conf, storm.createOzaBagLearner(tridentconfig).build());
		
		/*LocalCluster cls = new LocalCluster();
		LocalDRPC local = new LocalDRPC();
		tridentconfig.put("rpc", local);
		
		cls.submitTopology(topologyName,  conf,storm.createOzaBag(tridentconfig).build());*/
				
	}

	private static void populate(ZipOutputStream zos, File file, String root) throws Throwable {
		File[] files = file.listFiles();
		for (File f : files)
		{
			if (f.isDirectory())
			{
				populate(zos,f,root);
				continue;
			}
			FileInputStream fin = new FileInputStream(f);
			// Add ZIP entry to output stream.
			String name = f.getAbsolutePath();
			name = name.substring(root.length()+1);
	        zos.putNextEntry(new ZipEntry(name));

	        // Transfer bytes from the file to the ZIP file
	        int len;
	        while ((len = fin.read(buffer)) > 0) {
	            zos.write(buffer, 0, len);
	        }
	        // Complete the entry
	        zos.closeEntry();
	        fin.close();
		}
		
	}
}

package performance;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import moa.trident.topology.StormClusterTopology;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.ReducerAggregator;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.rapportive.storm.spout.AMQPSpout;

public class DeployAggregate {
	
	
	private static String STORM_HOME="/home/bsp/storm-0.8.2-wip8";
	static byte buffer[] = new byte[1024];
	
	
	
	public static void main(String[] args) throws Throwable
	{
		Properties prp = new Properties();
		prp.load(DeployAggregate.class.getResourceAsStream("/ml_storm_cluster.properties"));

		
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
	    
		TridentTopology topology = new TridentTopology();
		conf.put(AMQPSpout.CONFIG_PREFETCH_COUNT, 1000000);
		StormClusterTopology storm = new StormClusterTopology("/ml_storm_cluster.properties");
		StateFactory factory = storm.createFactory("sss");
		
		Stream s = storm.createLearningStream(null, topology);
		s.each(new Fields("instance"), new BaseFilter(){

			@Override
			public boolean isKeep(TridentTuple tuple) {
				// TODO Auto-generated method stub
				return true;
			}
			
		}).parallelismHint(30).persistentAggregate(factory, new ReducerAggregator<Integer>(){

			@Override
			public Integer init() {
				// TODO Auto-generated method stub
				return 1;
			}

			@Override
			public Integer reduce(Integer curr, TridentTuple tuple) {
				// TODO Auto-generated method stub
				return 1;
			}
			
		}, new Fields("output"));
		StormSubmitter.submitTopology(topologyName, conf, topology.build());
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

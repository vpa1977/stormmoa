package moa.storm.topology.test.event_emitter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.ReducerAggregator;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

class RRR implements ReducerAggregator<String>
{
	private static int id;
	private static int id_cnt=0;

	public RRR(int id)
	{
		this.id = id;
		id_cnt ++;
	}

	
	public String init() {
		// TODO Auto-generated method stub
		return null;
	}

	public String reduce(String curr, TridentTuple tuple) {
		System.out.println("I am reducer "+ id_cnt + " and processing "+ tuple);
		return null;
	}
	
}

public class Deploy {
	
	
	private static String STORM_HOME="/home/bsp/storm-0.8.2-wip8";
	static byte buffer[] = new byte[1024];
	
	public static void doSample(TridentTopology topology) throws InterruptedException
	{
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
	               new Values(new StringBuffer("1")),
	               new Values(new StringBuffer("2")),
	               new Values(new StringBuffer("3")),
	               new Values(new StringBuffer("4")) );
	   spout.setCycle(true);
	   topology.newStream("spout1", spout).partitionBy(new Fields("sentence")).each(new Fields("sentence"), new BaseFilter(){

		@Override
		public boolean isKeep(TridentTuple tuple) {
			StringBuffer inte =(StringBuffer) tuple.getValue(0);
			inte.append("asdasd");
					
			return true;
		}
		   
	   })
			.persistentAggregate(new MemoryMapState.Factory(), new Fields("sentence"), new RRR(1), new Fields("count")).parallelismHint(10);
			
			
			
			LocalCluster lolCluster = new LocalCluster();
			
			lolCluster.submitTopology("lolpology", new Config(), topology.build());
			Thread.sleep(10000000);
			   
	}
	
	public static void main(String[] args) throws Throwable
	{
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream("topology.jar"));
		populate(zos, new File("bin"), new File("bin").getAbsolutePath());
		zos.close();
		System.setProperty("storm.home", STORM_HOME);
		System.setProperty("storm.jar", "topology.jar");
		Config conf = new Config();
		
		TridentTopology topology = new TridentTopology();
		////
		doSample(topology);
		//topology.newDRPCStream("test").shuffle().parallelismHint(10);
		
		//StormSubmitter.submitTopology("test_topology", conf, topology.build());
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

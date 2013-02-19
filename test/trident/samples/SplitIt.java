package trident.samples;

import java.io.Serializable;
import java.util.HashMap;

import performance.TopologyPrinter;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TrackedTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/** 
 * Demonstrates Trident State Quering
 *
 */
public class SplitIt implements Serializable{
	
	 private static final class LocalTestJob implements TestJob, Serializable {
		@Override
		public void run(ILocalCluster cluster) throws Exception {
			TridentTopology topology = new TridentTopology();
			
			AckTracker tracker = new AckTracker(); 
			FeederSpout spout = new FeederSpout(new Fields("num"));
			spout.setAckFailDelegate(tracker);
			
			FeederSpout update = new FeederSpout(new Fields("num"));
			update.setAckFailDelegate(tracker);
			
			HashMap<String,String> map = new HashMap<String,String>();
			map.put("a","b");
			
			Debug filter = new Debug();
			Stream stream = topology.newStream("stream", spout);
			Stream[] streams = new Stream[10];
			for (int i = 0; i < 10 ; i ++ )
				streams[i] = stream.each(new Fields("num"), filter).shuffle();
			
			
			Stream merged = topology.merge(streams).parallelismHint(2);
			
			TopologyPrinter printer = new TopologyPrinter();
			try {
				printer.print(topology.build());
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			TrackedTopology tracked = Testing.mkTrackedTopology(cluster,topology.build());
			
			Config conf = new Config();
			conf.setNumAckers(1);
			conf.setNumWorkers(1);
			conf.setMaxSpoutPending(1);
			conf.put("topology.spout.max.batch.size", 1);
			cluster.submitTopology("test",conf, tracked.getTopology());
			spout.feed(new Values("A"));
			Testing.trackedWait(tracked, 1);
			tracker.resetNumAcks();
		}
	}


	public static void main(String[] args){
	
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(1);
		mkClusterParam.setPortsPerSupervisor(1);

		
		Testing.withTrackedCluster(mkClusterParam, new LocalTestJob());
	}
}

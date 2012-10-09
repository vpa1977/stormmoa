package performance;

import java.io.IOException;

import moa.storm.topology.StormClusterTopology;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;

public class LocalDeploy {
	public static void main(String[] args) throws IOException
	{
		LocalCluster local = new LocalCluster();
		TridentTopology topology = new TridentTopology();
		
		StormClusterTopology storm = new StormClusterTopology("/ml_storm_cluster.properties");
		Stream s = storm.createLearningStream(null, topology);
		s.each(new Fields("instance"), new BaseFilter(){

			@Override
			public boolean isKeep(TridentTuple tuple) {
				// TODO Auto-generated method stub
				return false;
			}
	
		
		});
		
		local.submitTopology("topology", new Config(), topology.build());
	}
}

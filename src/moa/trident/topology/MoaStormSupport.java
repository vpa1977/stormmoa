package moa.trident.topology;

import java.util.Map;

import storm.trident.state.StateFactory;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;

public class MoaStormSupport {
	private static LocalDRPC LOCAL_DRPC = new LocalDRPC();
	private static LocalCluster LOCAL_CLUSTER = new LocalCluster();
	
	public static LocalDRPC drpc()
	{
		return LOCAL_DRPC;
	}
	
	public static LocalCluster localCluster()
	{
		return LOCAL_CLUSTER;
	}
	
	public static void submit(String name, Map conf, StormTopology topology)
	{
		localCluster().submitTopology(name, conf, topology);
		
	}
	
	
	
	static 
	{
		startLocalMemcacheInstance(10001);
	}
	
	private static void startLocalMemcacheInstance(int port) {
    }

	public static StateFactory stateFactory() {
		return null;//MemcachedState.nonTransactional(Arrays.asList(new InetSocketAddress("localhost", 10001)));
	}
	
}

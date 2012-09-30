package moa.storm.tasks;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

import storm.trident.state.StateFactory;
import trident.memcached.MemcachedState;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;

import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
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
	
	private static final MemCacheDaemon<LocalCacheElement> daemon =
            new MemCacheDaemon<LocalCacheElement>();
	
	static 
	{
		startLocalMemcacheInstance(10001);
	}
	
	private static void startLocalMemcacheInstance(int port) {
        
        if (!daemon.isRunning())
        {
        	System.out.println("Starting local memcache");
        	CacheStorage<Key, LocalCacheElement> storage =
        			ConcurrentLinkedHashMap.create(
                        ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 100, 1024*500);
        	daemon.setCache(new CacheImpl(storage));
        	daemon.setAddr(new InetSocketAddress("localhost", port));
        	daemon.start();
        }
    }

	public static StateFactory stateFactory() {
		return MemcachedState.nonTransactional(Arrays.asList(new InetSocketAddress("localhost", 10001)));
	}
	
}

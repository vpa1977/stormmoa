package performance.ozaboost_distributed;


import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;


import moa.classifiers.Classifier;
import moa.storm.persistence.CassandraState;
import moa.storm.persistence.HDFSState;
import moa.storm.persistence.IStateFactory;
import moa.storm.topology.AllGrouping;
import moa.storm.topology.IdBasedGrouping;
import moa.storm.topology.MOAStreamSpout;
import moa.streams.generators.RandomTreeGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import performance.AllLocalGrouping;
import performance.CombinerBolt;
import performance.EchoBolt;
import performance.LocalGrouping;
import performance.cassandra.InstanceStreamSource;
import performance.cassandra.SharedStorageBolt;
import performance.cassandra.bolts.EvaluateSpout;
import performance.cassandra.bolts.TopologyBroadcastBolt;
import performance.cassandra.bolts.EvaluateClassifierBolt;
import performance.cassandra.bolts.LearnSpout;
import performance.cassandra.bolts.TrainClassifierBolt;
import performance.cassandra.bolts.WorkerBroadcastBolt;

import performance.ozaboost_distributed.bolts.BoostingLearningSpout;
import performance.state.DummyPersistentState;
import performance.state.DummyStateFactory;
import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EvaluateBoostedStorm extends BoostPartitionStorm implements Serializable {

	
	public EvaluateBoostedStorm(Config conf,String[] args) throws Throwable
	{
		
		super();

		final int ensemble_size = Integer.parseInt(args[0]);
		final int num_workers = Integer.parseInt(args[1]);
		final int num_classifiers = Integer.parseInt(args[2]);
		final int num_combiners = Integer.parseInt(args[3]);
		final int num_aggregators = Integer.parseInt(args[4]);
		final int num_pending = Integer.parseInt(args[5]);
		
		
		if ("true".equals(System.getProperty("localmode")))
		{
			MkClusterParam mkClusterParam = new MkClusterParam();
			mkClusterParam.setSupervisors(1);
			mkClusterParam.setPortsPerSupervisor(1);

			
			Testing.withLocalCluster(mkClusterParam, new TestJob(){

				@Override
				public void run(ILocalCluster cluster) throws Exception {

				//	CassandraState.Options<String> options = new CassandraState.Options<String>();
				//	IStateFactory cassandra = new CassandraState.Factory("localhost:9160", options );

//					HashMap<String,String> hdfs = new HashMap<String,String>();
					//hdfs.put("fs.default.name", "hdfs://localhost:9000");
					//hdfs.put("dfs.replication", "1");
					//IStateFactory cassandra = new HDFSState.Factory(hdfs);
					
					DummyPersistentState dummy_state = new DummyPersistentState();
					DummyStateFactory dummy_factory = new DummyStateFactory();
					dummy_factory.the_state = dummy_state;
					
					IStateFactory cassandra = dummy_factory;
					
					
					TopologyBuilder builder = new TopologyBuilder();
					RandomTreeGenerator stream = new RandomTreeGenerator();
					stream.prepareForUse();
					MOAStreamSpout moa_stream = new MOAStreamSpout(stream, 0);

					stream = new RandomTreeGenerator();
					stream.prepareForUse();
					MOAStreamSpout evaluate_stream = new MOAStreamSpout(stream, 0);
					
					buildLearnPart(cassandra,moa_stream, builder,"trees.HoeffdingTree -m 1000000 -e 10000", num_workers, ensemble_size, num_classifiers);
//					buildEvaluatePart(cassandra,evaluate_stream, builder, num_workers, ensemble_size, num_classifiers, num_classifiers, num_aggregators);
					//builder.setBolt("calculate_performance", new CounterBolt(),num_workers).customGrouping("aggregate_result", new LocalGrouping(new IdBasedGrouping()));		
					
					
					Config conf = new Config();
					conf.setNumAckers(num_workers);
					conf.setNumWorkers(num_workers);
					conf.setMaxSpoutPending(num_pending);
					cluster.submitTopology("test",conf, builder.createTopology());	
					Thread.sleep(10000000);
				}
				
			});
		}
		else 
		{
			
			conf.setNumAckers(num_workers);
			conf.setNumWorkers(num_workers);
			conf.setMaxSpoutPending(num_pending);
			conf.put("topology.worker.childopts", "-javaagent:/research/vp37/storm-0.8.2-wip8/lib/sizeofag-1.0.0.jar");
			conf.put("topology.message.timeout.secs", 60);
			
//			CassandraState.Options<String> options = new CassandraState.Options<String>();
//			IStateFactory cassandra = new CassandraState.Factory("ml64-1:9160", options );
			
			HashMap<String,String> hdfs = new HashMap<String,String>();
			hdfs.put("fs.default.name", "hdfs://ml64-1:9000");
			hdfs.put("dfs.replication", "1");
			IStateFactory cassandra = new HDFSState.Factory(hdfs);


			TopologyBuilder builder = new TopologyBuilder();
			RandomTreeGenerator stream = new RandomTreeGenerator();
			stream.prepareForUse();
			MOAStreamSpout moa_stream = new MOAStreamSpout(stream, 100);

			stream = new RandomTreeGenerator();
			stream.prepareForUse();
			MOAStreamSpout evaluate_stream = new MOAStreamSpout(stream, 0);
			
//				buildLearnPart(cassandra,moa_stream, builder,"trees.HoeffdingTree -m 10000000 -e 10000", num_workers, ensemble_size, num_classifiers);
				
//				StormSubmitter.submitTopology("learn"+ System.currentTimeMillis(), conf, builder.createTopology());
			builder = new TopologyBuilder();
			buildEvaluatePart(cassandra,evaluate_stream, builder, num_workers, ensemble_size, num_classifiers, num_classifiers, num_aggregators);
			builder.setBolt("calculate_performance", new CounterBolt(),num_workers).customGrouping("aggregate_result", new LocalGrouping(new IdBasedGrouping()));		
			StormSubmitter.submitTopology("evaluate"+ System.currentTimeMillis(), conf, builder.createTopology());

			
			
		}
	}
	

	public static void main(String[] args) throws Throwable
	{
		
		Config conf = new Config();
		new EvaluateBoostedStorm(conf,args);
	}

}

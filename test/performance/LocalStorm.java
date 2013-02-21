package performance;


import java.io.Serializable;

import moa.storm.kryo.WekaSerializers;
import moa.storm.topology.grouping.IdBasedGrouping;
import moa.storm.topology.meta.MemoryOnlyOzaBag;
import moa.storm.topology.meta.MoaConfig;
import moa.streams.generators.RandomTreeGenerator;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class LocalStorm extends MemoryOnlyOzaBag implements Serializable {

	public static final long GLOBAL_BATCH_SIZE = 100;
	
		static int INSTANCE = 0;
	public static Logger LOG = Logger.getLogger(LocalStorm.class);

	
	public LocalStorm(MoaConfig conf,String[] args) throws Throwable
	{
		super();
		
		
		
		TopologyBuilder builder = new TopologyBuilder();
		
		conf.setEnsembleSize(Integer.parseInt(args[0]));
		conf.setNumWorkers(Integer.parseInt(args[1]));
		conf.setNumClassifierExecutors(Integer.parseInt(args[2]));
		conf.setNumCombiners(Integer.parseInt(args[3]));
		conf.setNumAggregators(Integer.parseInt(args[4]));
		conf.setMaxSpoutPending(Integer.parseInt(args[5]));
		conf.setNumAckers(conf.getNumWorkers());
		
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		
		stream = new RandomTreeGenerator();
		stream.prepareForUse();
		MOAStreamSpout predict = new MOAStreamSpout(stream, 0);
		
		MOAStreamSpout learn = new MOAStreamSpout(stream, 100);
		m_header = stream.nextInstance().dataset();
		build("trees.HoeffdingTree -m 10000000 -e 10000",
				builder, conf, learn,predict);
		
		builder.setBolt("calculate_performance", new CounterBolt(),conf.getNumWorkers())
			.customGrouping("prediction_result", new LocalGrouping(new IdBasedGrouping()));
		
		
		
		WekaSerializers.register(conf);
		
		
		if ("true".equals(System.getProperty("localmode")))
		{
			LocalCluster local = new LocalCluster();
			//conf.setMaxSpoutPending(1);
			//conf.put("topology.spout.max.batch.size", 2);
			local.submitTopology("test",conf, builder.createTopology());
		}
		else
		{
			StormSubmitter.submitTopology("noack"+ System.currentTimeMillis(), conf, builder.createTopology());
		}
		
		
	}


	public static void main(String[] args) throws Throwable
	{
		
		MoaConfig conf = new MoaConfig();
		LocalStorm storm = new LocalStorm(conf,args);
	}

}

package performance;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import moa.storm.kryo.WekaSerializers;
import moa.storm.topology.grouping.IdBasedGrouping;
import moa.storm.topology.meta.MemoryOnlyOzaBag;
import moa.streams.generators.RandomTreeGenerator;

import org.apache.log4j.Logger;


import weka.core.Instance;
import weka.core.Instances;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class LocalStorm extends MemoryOnlyOzaBag implements Serializable {

	public static final long GLOBAL_BATCH_SIZE = 100;
	
		static int INSTANCE = 0;
	public static Logger LOG = Logger.getLogger(LocalStorm.class);

	
	class CounterBolt extends BaseRichBolt implements IRichBolt
	{
		


		private OutputCollector m_collector;

		
		long m_instance;
		long m_start =0;
		long m_measurement_start = 0;
		long count = 0;
		long period = 0;
		
		final long MEASUREMENT_PERIOD = 1 * 60 * 1000;
	
		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			
		}
		
		private int getPid() throws Throwable
		{
			java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
			java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
			jvm.setAccessible(true);
			sun.management.VMManagement mgmt = (sun.management.VMManagement) jvm.get(runtime);
			java.lang.reflect.Method pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
			pid_method.setAccessible(true);
			int pid = (Integer) pid_method.invoke(mgmt);
			return pid;
		}			


		
		private void writeResult(long period)
		{
			try {
				
				long tup_sec = count * GLOBAL_BATCH_SIZE * 1000 /period;
				
				File f = new File("/home/vp37/trident_bench"+ InetAddress.getLocalHost().getHostName() + "-" + getPid() + "-" + m_instance);
				FileOutputStream fos = new FileOutputStream(f);
				String result = "" +tup_sec;
				fos.write(result.getBytes());
				fos.write(" \r\n".getBytes());
				fos.flush(); 
				fos.close();
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}
		}

		@Override
		public void execute(Tuple tuple) {
			m_collector.ack(tuple);
			if (m_start == 0 ) 
				m_start = System.currentTimeMillis();
			long current =System.currentTimeMillis(); 
			count ++;
			if (count %10000 == 0)
				System.out.println("processed "+ count);
			if (current - m_start > MEASUREMENT_PERIOD)
			{
				LOG.info("Writing Result");
				writeResult(current - m_start);
				m_start = System.currentTimeMillis();
				count = 0;
			}  
			
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			m_collector = collector;
			m_instance = INSTANCE ++;
			System.out.println("New Instance "+ INSTANCE);
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	
	public LocalStorm(Config conf,String[] args) throws Throwable
	{
		super();
		
		TopologyBuilder builder = new TopologyBuilder();
		int ensemble_size = Integer.parseInt(args[0]);
		int num_workers = Integer.parseInt(args[1]);
		int num_classifiers = Integer.parseInt(args[2]);
		int num_combiners = Integer.parseInt(args[3]);
		int num_aggregators = Integer.parseInt(args[4]);
		int num_pending = Integer.parseInt(args[5]);

		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		
		stream = new RandomTreeGenerator();
		stream.prepareForUse();
		MOAStreamSpout predict = new MOAStreamSpout(stream, 0);
		MOAStreamSpout learn = new MOAStreamSpout(stream, 100);
		m_header = stream.nextInstance().dataset();
		build("trees.HoeffdingTree -m 10000000 -e 10000",
				builder, ensemble_size, num_workers, num_classifiers,
				num_combiners, num_aggregators, learn,predict);
		
		builder.setBolt("calculate_performance", new CounterBolt(),num_workers).customGrouping("prediction_result", new LocalGrouping(new IdBasedGrouping()));
		
		conf.setNumAckers(num_workers);
		conf.setNumWorkers(num_workers);
		WekaSerializers.register(conf);
		
		conf.setMaxSpoutPending(num_pending);
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
		
		Config conf = new Config();
		LocalStorm storm = new LocalStorm(conf,args);
	}

}

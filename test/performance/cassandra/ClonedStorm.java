package performance.cassandra;


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

import moa.storm.cassandra.CassandraState;
import moa.storm.topology.AllGrouping;
import moa.storm.topology.EvaluateClassifierBolt;
import moa.storm.topology.EvaluateSpout;
import moa.storm.topology.IdBasedGrouping;
import moa.storm.topology.LearnSpout;
import moa.storm.topology.MOAStreamSpout;
import moa.storm.topology.TrainClassifierBolt;
import moa.streams.generators.RandomTreeGenerator;

import org.apache.log4j.Logger;

import performance.AllLocalGrouping;
import performance.CombinerBolt;
import performance.EchoBolt;
import performance.LocalGrouping;
import storm.trident.state.StateFactory;
import weka.core.Instance;
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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ClonedStorm implements Serializable {

	public static final long GLOBAL_BATCH_SIZE = 100;
	
	class DeserializeBolt extends BaseRichBolt implements IRichBolt
	{
		private long m_instance_id;
		private OutputCollector m_collector;
		private int m_ensemble_size;
		int m_task_id;
		private String m_stream_id;
		public DeserializeBolt(String streamId)
		{
			m_stream_id = streamId;
		}
		
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			m_collector = collector;
			m_instance_id = 0;
			int task_id = context.getThisTaskId();
			m_task_id = task_id;
		}

		@Override
		public void execute(Tuple tuple) {
			Object value = tuple.getValue(0);
			List<Instance> inst = null;
			if (value instanceof String){
				byte[] b = DatatypeConverter.parseBase64Binary(String.valueOf(value));
		        ObjectInputStream is;
		        Object serializedObject = null;
				try {
					is = new ObjectInputStream( new ByteArrayInputStream(b));
					serializedObject = is.readObject();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				inst = (List<Instance>)serializedObject;
			}
			else if (value instanceof List)	{
				inst = (List<Instance>)value;
			} else{
				throw new RuntimeException("Cannot deserialize "+ value);
			}
			m_instance_id++;
			ArrayList<Object> output = new ArrayList<Object>();
			
			output.add(new moa.storm.topology.MessageIdentifier(m_task_id, m_instance_id));
			output.add(inst);
			m_collector.emit(m_stream_id,tuple,output);
			m_collector.ack(tuple);
			if (m_instance_id % 10000 == 0)
			{
				System.out.println("Deserialized "+ m_instance_id+ " on "+ m_task_id);
			}
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream(m_stream_id,new Fields("instance_id", "instance"));
		}
		
	}
	
	static int INSTANCE = 0;
	public static Logger LOG = Logger.getLogger(ClonedStorm.class);

	
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
	
	public void buildLearnPart(StateFactory cassandra,IRichSpout source, TopologyBuilder builder,String clasisifer_cli, int num_workers, int ensemble_size, int num_classifier_executors)
	{
		LearnSpout learn_spout = new LearnSpout(source, cassandra, 100);
		builder.setSpout("learner_stream",  learn_spout);
		builder.setBolt("deserialize", new DeserializeBolt("learn"),num_workers).shuffleGrouping("learner_stream");
		builder.setBolt("learn_local_grouping", new EchoBolt("learn"), num_workers)
			.customGrouping("deserialize", "learn", new AllGrouping());
			
		
		builder.setBolt("train_classifier", new TrainClassifierBolt(clasisifer_cli,cassandra ),Math.max(num_classifier_executors,num_workers))
			.setNumTasks(ensemble_size)
			.customGrouping("learn_local_grouping", "learn", new AllLocalGrouping())
			.customGrouping("learner_stream", LearnSpout.NOTIFICATION_STREAM, new AllGrouping());
	}
	
	public void buildEvaluatePart(StateFactory cassandra,IRichSpout source, TopologyBuilder builder,int num_workers, int ensemble_size, 
			int num_classifier_executors, int num_combiners, int num_aggregators)
	{
		EvaluateSpout evaluate_spout = new EvaluateSpout(source, cassandra, 100);
		builder.setSpout("prediction_stream", evaluate_spout);
		builder.setBolt("p_deserialize", new DeserializeBolt("evaluate"),num_workers).shuffleGrouping("prediction_stream");
		
		builder.setBolt("evaluate_local_grouping", new EchoBolt("evaluate"), num_workers).customGrouping("p_deserialize", "evaluate", new AllGrouping());

		builder.setBolt("evaluate_classifier", new EvaluateClassifierBolt(cassandra),Math.max(num_classifier_executors,num_workers))
			.customGrouping("evaluate_local_grouping", "evaluate", new AllLocalGrouping())
			.customGrouping("prediction_stream", EvaluateSpout.NOTIFICATION_STREAM, new AllGrouping())
			.setNumTasks(ensemble_size);
		
		builder.setBolt("combine_result", new CombinerBolt ("evaluate_classifier"), Math.max(num_workers, num_combiners))
			.customGrouping("evaluate_classifier", new LocalGrouping( new IdBasedGrouping()))
			.setNumTasks(Math.max(num_workers, num_combiners));
	
		builder.setBolt("aggregate_result", new CombinerBolt(ensemble_size), Math.max(num_workers, num_combiners))
			.customGrouping("combine_result", new IdBasedGrouping())
			.setNumTasks(Math.max(num_workers, num_aggregators) );

		
	}
	
	
	public ClonedStorm(Config conf,String[] args) throws Throwable
	{
		
		

		final int ensemble_size = Integer.parseInt(args[0]);
		final int num_workers = Integer.parseInt(args[1]);
		final int num_classifiers = Integer.parseInt(args[2]);
		final int num_combiners = Integer.parseInt(args[3]);
		final int num_aggregators = Integer.parseInt(args[4]);
		final int num_pending = Integer.parseInt(args[5]);
		
		
		if ("true".equals(System.getProperty("localmode")))
		{
			MkClusterParam mkClusterParam = new MkClusterParam();
			mkClusterParam.setSupervisors(2);
			mkClusterParam.setPortsPerSupervisor(5);

			
			Testing.withLocalCluster(mkClusterParam, new TestJob(){

				@Override
				public void run(ILocalCluster cluster) throws Exception {

					CassandraState.Options<String> options = new CassandraState.Options<String>();
					StateFactory cassandra = new CassandraState.Factory("localhost:9160", options );
					
					TopologyBuilder builder = new TopologyBuilder();
					RandomTreeGenerator stream = new RandomTreeGenerator();
					stream.prepareForUse();
					MOAStreamSpout moa_stream = new MOAStreamSpout(stream, 100);

					stream = new RandomTreeGenerator();
					stream.prepareForUse();
					MOAStreamSpout evaluate_stream = new MOAStreamSpout(stream, 0);
					
					buildLearnPart(cassandra,moa_stream, builder,"trees.HoeffdingTree", num_workers, ensemble_size, num_classifiers);
					buildEvaluatePart(cassandra,evaluate_stream, builder, num_workers, ensemble_size, num_classifiers, num_classifiers, num_aggregators);
					builder.setBolt("calculate_performance", new CounterBolt(),num_workers).customGrouping("aggregate_result", new LocalGrouping(new IdBasedGrouping()));		
					
					
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
			CassandraState.Options<String> options = new CassandraState.Options<String>();
			StateFactory cassandra = new CassandraState.Factory("ml64-1:9160", options );

			TopologyBuilder builder = new TopologyBuilder();
			RandomTreeGenerator stream = new RandomTreeGenerator();
			stream.prepareForUse();
			MOAStreamSpout moa_stream = new MOAStreamSpout(stream, 100);

			stream = new RandomTreeGenerator();
			stream.prepareForUse();
			MOAStreamSpout evaluate_stream = new MOAStreamSpout(stream, 0);
			
			buildLearnPart(cassandra,moa_stream, builder,"trees.HoeffdingTree", num_workers, ensemble_size, num_classifiers);
			buildEvaluatePart(cassandra,evaluate_stream, builder, num_workers, ensemble_size, num_classifiers, num_classifiers, num_aggregators);
			builder.setBolt("calculate_performance", new CounterBolt(),num_workers).customGrouping("aggregate_result", new LocalGrouping(new IdBasedGrouping()));		
			
			conf.setNumAckers(num_workers);
			conf.setNumWorkers(num_workers);
			conf.setMaxSpoutPending(num_pending);
			
			StormSubmitter.submitTopology("noack"+ System.currentTimeMillis(), conf, builder.createTopology());
		}
	}
	

	public static void main(String[] args) throws Throwable
	{
		
		Config conf = new Config();
		ClonedStorm storm = new ClonedStorm(conf,args);
	}

}

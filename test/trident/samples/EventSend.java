package trident.samples;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import moa.storm.topology.grouping.AllGrouping;
import moa.streams.generators.RandomTreeGenerator;
import weka.core.DenseInstance;
import weka.core.Instance;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;

/** 
 * Demonstrates Trident State Quering
 *
 */
public class EventSend implements Serializable{
	
	static int THE_SIZE = 25000;
	
	private static class PassMeBolt extends BaseRichBolt
	{
		private OutputCollector m_collector;
		private boolean last;
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			m_collector = collector;
			last = context.getThisTaskId() == context.getComponentTasks(context.getThisComponentId()).size()-1;
		}

		@Override
		public void execute(Tuple input) {
			Integer value = input.getInteger(1);
			long time = input.getLong(0);
			if (time == 0)
				time = System.currentTimeMillis();
			ArrayList output = new ArrayList();
			output.add(time);
			output.add(new Integer(value.intValue()+1));
			output.add(input.getValue(2));
			m_collector.emit(input, output);
			
			if (last){
				m_collector.emit("knock",input, output);
			}
			m_collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("1","2","3"));
			declarer.declareStream("knock", new Fields("1","2","3"));
		}
		
	}
	
	private static class DeadEnd extends BaseRichBolt
	{
		static int count;
		static long fin = System.currentTimeMillis();
		static long start = System.currentTimeMillis();
		private OutputCollector m_collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			m_collector = collector;
			
		}

		@Override
		public void execute(Tuple input) {
			
			
			Integer value = input.getInteger(1);
			//System.out.println("emit" + (value.intValue()+1));
			final Instance inst = (Instance)input.getValue(2);
			count ++;
			if (count % THE_SIZE == 0)
			{
				fin = System.currentTimeMillis();
				System.out.println("Time From Timestamp" + (fin -input.getLong(0)));
				System.out.println("Time Between Events" + (fin -start));
				start = System.currentTimeMillis();
				
				
			}
			m_collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
		
	}
	private static Instance aaa_inst;
	
	 private static final class LocalTestJob implements TestJob, Serializable {
		@Override
		public void run(ILocalCluster cluster) throws Exception {
			TopologyBuilder topology = new TopologyBuilder();
			
			AckTracker tracker = new AckTracker(); 
			FeederSpout spout = new FeederSpout(new Fields("1","2","3"));
			spout.setAckFailDelegate(tracker);
			
			topology.setSpout("feed", spout,1);
			/*	topology.setBolt("entrance", new PassMeBolt(),1).shuffleGrouping("feed");
	     	topology.setBolt("executor", new PassMeBolt(),8).customGrouping("entrance", new PassGrouping())
															.customGrouping("executor", new PassGrouping()).setNumTasks(THE_SIZE);
			topology.setBolt("finis", new DeadEnd(),1).shuffleGrouping("executor", "knock");
			*/
			topology.setBolt("entrance", new PassMeBolt(),1).shuffleGrouping("feed");
			topology.setBolt("executor", new PassMeBolt(),1).customGrouping("entrance", new AllGrouping()).setNumTasks(THE_SIZE);
			topology.setBolt("finis", new DeadEnd(),1).shuffleGrouping("executor");
			
			//TrackedTopology tracked = Testing.mkTrackedTopology(cluster,topology.createTopology());
			Config conf = new Config();
			conf.setNumAckers(1);
			conf.setNumWorkers(1);
			conf.setMaxSpoutPending(1);
			RandomTreeGenerator theGenerator = new RandomTreeGenerator();
			theGenerator.numNumericsOption.setValue(100);
			theGenerator.prepareForUse();
			cluster.submitTopology("test",conf, topology.createTopology());
			boolean bom = false;
			for (int i = 0 ;i < 20 ; i ++ )
			{
				aaa_inst = theGenerator.nextInstance();
				spout.feed(new Values(new Long(0),0,aaa_inst),1);
				while (tracker.getNumAcks() == 0)
					Thread.sleep(500);
				tracker.resetNumAcks();
			}
			Thread.sleep(1000000);
			

		}
	}


	public static void main(String[] args){
	
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(1);
		mkClusterParam.setPortsPerSupervisor(1);
		
		DenseInstance lol = new DenseInstance(1, new double[]{1,1,1,1,1,1});
		
		RandomTreeGenerator theGenerator = new RandomTreeGenerator();
		theGenerator.numNumericsOption.setValue(100);
		theGenerator.prepareForUse();
		lol = (DenseInstance)theGenerator.nextInstance();
		Kryo kryo = new Kryo();
		double start = System.currentTimeMillis();
		for (int i = 0; i < 100000 ; i ++ )
		{
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			//Output out = new Output(bos);
			//kryo.writeObject(out, lol);
			ObjectOutputStream os;
			try {
				os = new ObjectOutputStream(bos);
				os.writeObject(lol);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		double end = System.currentTimeMillis();
		System.out.println("time "+ (end-start)/100000);

		
		//Testing.withLocalCluster(mkClusterParam, new LocalTestJob());
	}
}


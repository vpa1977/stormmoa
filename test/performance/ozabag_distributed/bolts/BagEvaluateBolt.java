package performance.ozabag_distributed.bolts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import performance.ozabag_distributed.BaggingMember;


import moa.classifiers.Classifier;
import moa.core.DoubleVector;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.SharedStorageBolt;
import moa.storm.topology.message.EnsembleCommand;
import moa.storm.topology.message.Reset;
import moa.trident.topology.LearnerWrapper;
import storm.trident.state.StateFactory;
import weka.core.Instance;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class BagEvaluateBolt extends BasePartition implements IRichBolt
{
	private OutputCollector m_collector;
	private String m_key;

	private transient ArrayList<BaggingMember> m_wrapper;
	private IStateFactory m_state_factory;
	private long m_version;
	private long m_pending;

	public BagEvaluateBolt(int ensemble_size,IStateFactory classifierState) {
		super(ensemble_size);
		m_state_factory = classifierState;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		m_collector = collector;
		m_wrapper = null;
		m_key = "classifier";
		m_version = -1;
		
	}

	@Override
	public void execute(Tuple tuple) {
		
		long version = tuple.getLongByField("version").longValue();
		if (version != m_version || m_wrapper == null ) 
		{
			m_version = version ;
			m_wrapper = new ArrayList<BaggingMember>();
			for (int i = m_start_key ; i < m_start_key + m_partition_size; i ++)
			{
				BaggingMember m = (BaggingMember)PartitionedSharedStorageBolt.instance().get(m_key+i, m_version);
				if (m == null){
					m_wrapper =null;
					throw new RuntimeException("Unable to fetch "+ m_key + i + " "+ m_version);
				} else {
					m_wrapper.add(m);
				}
			}
			
		}
		
		if (m_wrapper != null) {
			Object instance_id = tuple.getValue(0);
			Object instance = tuple.getValue(1);
			
			List<Object> objs = new ArrayList<Object>();
			objs.add(instance_id);
			objs.add(instance );
			ArrayList< DoubleVector > results = new ArrayList<DoubleVector>();
			List<Instance> list = (List<Instance>) instance;
			Iterator<Instance> it = list.iterator();
			while (it.hasNext())
			{
				Instance inst = it.next();
				DoubleVector sum = new DoubleVector();
				for (int i = 0 ; i< m_wrapper.size() ; i ++)
				{
					BaggingMember m = m_wrapper.get(i);
					DoubleVector vote =new DoubleVector(m.m_classifier.getVotesForInstance( inst ));
					if (vote.sumOfValues() > 0.0) 
					{
	                    vote.normalize();
	                    sum.addValues(vote);
	                }
				}
				results.add(sum);
			}
	
			objs.add(results);
			objs.add(m_wrapper.size());
			objs.add(System.currentTimeMillis());			
			m_collector.emit(tuple,objs);
		}
		else {
			throw new RuntimeException("Storage is down");
		}
		
		
		m_collector.ack(tuple);
	}
		

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance_id", "instance", "prediction","votes","timestamp"));
	}
}
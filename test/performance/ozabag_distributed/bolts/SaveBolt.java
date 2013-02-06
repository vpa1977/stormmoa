package performance.ozabag_distributed.bolts;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import performance.ozabag_distributed.BaggingMember;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SaveBolt extends BaseRichBolt implements IRichBolt
{
	
	private transient IPersistentState<BaggingMember> m_classifier_state;
	private IStateFactory m_state_factory;
	private OutputCollector m_collector;
	
	public SaveBolt(IStateFactory fact ) 
	{
		m_state_factory = fact;
	}
	
	/*
	public static SaveBolt instance()
	{
		return m_instance;
	}*/

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
		m_classifier_state = ((IStateFactory) m_state_factory).create();	
		
	}

	@Override
	public void execute(Tuple input) {
		long version = input.getLongByField("version");
		List<BaggingMember> ensemble_partition = (List<BaggingMember>)input.getValueByField("ensemble_partition");
		System.out.println("Saving version "+ version + " with "+ ensemble_partition.size() + " elements ");
		for (BaggingMember m : ensemble_partition)
		{
			m_classifier_state.put(m.m_key,String.valueOf(version), m);
		}
		
		m_collector.emit("persist_notify", input, input.select(new Fields("version")));
		m_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declareStream("persist_notify", new Fields("version")); 
	}
	

}

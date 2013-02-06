package performance.ozaboost_distributed.bolts;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import performance.ozaboost_distributed.BoostingMember;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SaveBolt extends BaseRichBolt implements IRichBolt
{
	
	private static ConcurrentHashMap<String, BoostingMember> m_map = new ConcurrentHashMap<String, BoostingMember>();
	private static SaveBolt m_instance; 
	private transient IPersistentState<BoostingMember> m_classifier_state;
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

		synchronized (this) {
			m_classifier_state = ((IStateFactory) m_state_factory).create();	
			if (m_instance ==null)
				m_instance = this;
		}
		
	}

	@Override
	public void execute(Tuple input) {
		long version = input.getLongByField("version");
		List<BoostingMember> ensemble_partition = (List<BoostingMember>)input.getValueByField("ensemble_partition");
		for (BoostingMember m : ensemble_partition)
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
	
	public void put(String m_key, long m_version, BoostingMember m_wrapper) {
		BoostingMember copy = m_wrapper;
		copy.m_classifier = copy.m_classifier.copy();
		m_map.put(m_key + "-" + m_version, copy);
	}

}

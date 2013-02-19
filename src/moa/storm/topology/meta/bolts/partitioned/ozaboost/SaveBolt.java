package moa.storm.topology.meta.bolts.partitioned.ozaboost;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.ensemble_members.EnsembleMember;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SaveBolt<T extends EnsembleMember> extends BaseRichBolt implements IRichBolt
{
	
	private static ConcurrentHashMap<String, Object> m_map = new ConcurrentHashMap<String, Object>();
	private static SaveBolt m_instance; 
	private transient IPersistentState<T> m_classifier_state;
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
		List<T> ensemble_partition = (List<T>)input.getValueByField("ensemble_partition");
		for (T m : ensemble_partition)
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
	
	public void put(String m_key, long m_version, T m_wrapper) {
		EnsembleMember copy = m_wrapper.copy();
		m_map.put(m_key + "-" + m_version, copy);
	}

}

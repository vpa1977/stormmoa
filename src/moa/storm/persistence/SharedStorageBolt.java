package moa.storm.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import moa.core.SizeOf;
import moa.storm.topology.message.EnsembleCommand;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/** 
 * Implements a read only updater
 *  singleton bolt/spout -> N updaters
 *  singleton bolt/spout  
 * @author bsp
 *
 * @param <T>
 */
public class SharedStorageBolt<T>  implements IRichBolt {
	
	private OutputCollector m_collector;
	protected IPersistentState<T> m_state;
	protected IStateFactory m_factory;
	private String m_user_component;
	protected List<Integer> m_managed_components;
	private static ConcurrentHashMap<String,Object> m_data = new ConcurrentHashMap<String, Object>();
	private static SharedStorageBolt m_instance;
	protected ArrayList<Long> m_versions;

	
	public SharedStorageBolt(IStateFactory factory, String user_component){
		m_factory = factory;
		m_user_component = user_component;
	}
	
	public static SharedStorageBolt instance()
	{
		if (m_instance == null)
			throw new RuntimeException("SharedStorageBolt is not instantiated");
		return m_instance;
	}
	
	
	public T get(String key, long version)
	{
		return (T)m_data.get(key + version);
	}
	
	public void writeStorage(String key, long version, Object o)
	{
		m_state.put(key, String.valueOf(version), o);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// @TODO: check the 
		m_collector =collector;
		m_state = m_factory.create();
		m_managed_components = new ArrayList<Integer>();
		List<Integer> alltasks = context.getThisWorkerTasks();
		List<Integer> classifiers = context.getComponentTasks(m_user_component);
		for (int i = 0 ; i < classifiers.size() ; i ++)
		{
			if ( alltasks.contains(classifiers.get(i)))
			{
					m_managed_components.add(i);
			}
		}
		
		
		if (m_instance == null)
			m_instance = this;
		m_versions = new ArrayList<Long>();
	}

	@Override
	public void execute(Tuple input) {
		EnsembleCommand cmd = (EnsembleCommand)input.getValue(0);
		long version_to_read = cmd.version();
		try {
			processVersion(version_to_read);
			m_collector.ack(input);
		}
		catch (RuntimeException e){
			m_collector.fail(input);
		}
	}

	public void processVersion(long version_to_read) {
		
		for (Integer n : m_managed_components)
		{
			if (m_data == null)
				throw new RuntimeException("m_data == null");
			
			//System.out.println("Fetch " + "classifier"+ String.valueOf(n) + " version "+ String.valueOf(version_to_read));
			Object value = m_state.get("classifier"+ String.valueOf(n), String.valueOf(version_to_read));
			
			if (value == null)
				throw new RuntimeException("Value == null for key " + "classifier"+ String.valueOf(n) + " version "+ String.valueOf(version_to_read));
			long value_size = SizeOf.fullSizeOf(value) / 1024;
			System.out.println("Fetch " + "classifier"+ String.valueOf(n) + " version "+ String.valueOf(version_to_read) + " size " + value_size);
			
			m_data.put("classifier"+ String.valueOf(n)+ String.valueOf(version_to_read), value);
			clean("classifier"+ String.valueOf(n));
		}
		
		long prev = -1;
		if (m_versions.size() > 0)
			 prev = m_versions.get( m_versions.size() -1);

		m_versions = new ArrayList<Long>();
		if (prev >= 0)
			m_versions.add(prev);
		m_versions.add(version_to_read);
		
		if (m_data.size() > m_managed_components.size() * 2)
			throw new RuntimeException("Too many objects in the shared storage");
	}

	private void clean(String key) {
		for (int i = 0 ; i < m_versions.size() -1 ; i ++ )
		{
			m_data.remove(key+ String.valueOf(m_versions.get(i)));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

package performance.ozaboost_distributed.bolts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import moa.core.SizeOf;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.IPersistentState;
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
public class PartitionedSharedStorageBolt<T>  implements IRichBolt {
	
	private OutputCollector m_collector;
	protected IPersistentState<T> m_state;
	protected IStateFactory m_factory;
	private String m_user_component;
	private long m_ensemble_size;
	private static ConcurrentHashMap<String,Object> m_data = new ConcurrentHashMap<String, Object>();
	
	private static PartitionedSharedStorageBolt m_instance;
	protected ArrayList<Long> m_versions;
	private long m_start_key;
	private long m_partition_size;
	
	private class Partition implements Serializable 
	{
		long m_start;
		long m_size;
	}
	private ArrayList<Partition> m_partitions;

	
	public PartitionedSharedStorageBolt(int ensemble_size,IStateFactory factory, String user_component){
		m_ensemble_size = ensemble_size;
		m_factory = factory;
		m_user_component = user_component;
	}
	
	public static PartitionedSharedStorageBolt instance()
	{
		if (m_instance == null)
			throw new RuntimeException("SharedStorageBolt is not instantiated");
		return m_instance;
	}
	
	
	public T get(String key, long version)
	{
		T ret = (T)m_data.get(key + version);
		if (ret == null){
			for(String s :m_data.keySet())
			{
				System.out.println("Known key "+ s);
			}
			throw new RuntimeException("Unknown key "+ (key+version)+ " my size is "+ m_data.size());
			
		}
		return ret;
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
		List<Integer> alltasks = context.getThisWorkerTasks();
		List<Integer> classifiers = context.getComponentTasks(m_user_component);
		m_partitions = new ArrayList<Partition>();
		int first_component = -1;
		int last_component = -1;
		int num_components =0;
		long size = m_ensemble_size /classifiers.size();
		System.out.println("Prepare Storage: "+ m_ensemble_size);
		System.out.println("Prepare Storage Tasks: "+ classifiers.size());
		for (int i = 0 ; i < classifiers.size() ; i ++)
		{
			if ( alltasks.contains(classifiers.get(i)))
			{
				Partition p = new Partition();
				p.m_start = i * size;
				p.m_size = size;
				if (i == classifiers.size()-1)
					p.m_size = m_ensemble_size - p.m_start;
				System.out.println("Add Partition: "+ i + " start "+ p.m_start + " size "+ p.m_size);		
				m_partitions.add(p);
			}
		}
		
		
		
		
		m_start_key =  size * first_component;
		boolean b_last = last_component == classifiers.size()-1;
		if (b_last)
			m_partition_size = m_ensemble_size - m_start_key;
		else
			m_partition_size = num_components*size; 
		
		
		
		if (m_instance == null)
		{
			System.out.println("instance update");
			m_instance = this;
		}
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
		// we may be asked to re-read same version, due to timeouts etc.
		if (m_versions.size() > 0 && m_versions.get(m_versions.size()-1) == version_to_read)
			return;
		
		for (Partition p : m_partitions)
			for (long n = p.m_start ; n < p.m_start + p.m_size; n ++)
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
		System.out.println("My data size is "+ m_data.size());
		if (m_data.size() > (m_partition_size) * 2)
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

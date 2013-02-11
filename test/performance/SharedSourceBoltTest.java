package performance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import junit.framework.TestCase;
import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.SharedStorageBolt;

import org.junit.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

public class SharedSourceBoltTest extends TestCase implements Serializable{
	private class DummyPersistentState implements IPersistentState
	{
		public long the_long;
		public Object the_object;

		@Override
		public long getLong(String row, String column) {
			// TODO Auto-generated method stub
			return the_long;
		}

		@Override
		public void setLong(String row, String column, long value) {
			the_long = value;
			
		}

		@Override
		public Object get(String row, String column) {
			// TODO Auto-generated method stub
			return the_object;
		}

		@Override
		public void put(String rowKey, String key, Object value) {
			the_object = value;
			
		}

		@Override
		public void deleteRow(String rowKey) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void deleteColumn(String rowKey, String columnKey) {
			// TODO Auto-generated method stub
			
		}
		
	}

	private class DummyStateFactory implements IStateFactory 
	{
		public IPersistentState the_state;

		@Override
		public IPersistentState create() {
			// TODO Auto-generated method stub
			return the_state;
		}
		
	}
	
	private class SharedStorageBoltProxy extends SharedStorageBolt 
	{

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			m_state = m_factory.create();
			m_managed_components = new ArrayList<Integer>();
			m_managed_components.add(1);
			m_versions = new ArrayList<Long>();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			super.declareOutputFields(declarer);
		}

		public SharedStorageBoltProxy(IStateFactory factory,
				String user_component) {
			super(factory, user_component);
			// TODO Auto-generated constructor stub
		}
		
	}
	
	
	
	@Test
	public void test()
	{	
		DummyPersistentState dummy_state = new DummyPersistentState();
		dummy_state.the_object = new Object(); 
		DummyStateFactory dummy_factory = new DummyStateFactory();
		dummy_factory.the_state = dummy_state;
		
		SharedStorageBoltProxy proxy= new SharedStorageBoltProxy(dummy_factory, "the_component");
		proxy.prepare(null, null,null);
		
		for (int i = 0; i < 100 ; i++)
			proxy.processVersion(i);
				
		
	}
	
	
}

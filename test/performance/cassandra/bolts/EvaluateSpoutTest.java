package performance.cassandra.bolts;

import static org.junit.Assert.*;


import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import moa.storm.topology.MOAStreamSpout;
import moa.storm.topology.MessageIdentifier;
import moa.streams.generators.RandomTreeGenerator;

import org.junit.Test;

import backtype.storm.task.TopologyContext;

import performance.cassandra.InstanceStreamSource;
import weka.classifiers.trees.RandomTree;

public class EvaluateSpoutTest {
	
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


	private class EvaluateSpoutProxy extends EvaluateSpout
	{

		public boolean update_emitted = false;
		public boolean evaluation_emitted = false;
		
		public void reset()
		{
			update_emitted = false;
			evaluation_emitted = false;
		}
		@Override
		protected void emitUpdateMessage() {
			update_emitted = true;
		}

		@Override
		protected void emitEvaluationMessage() {
			evaluation_emitted = true;
		}

		public EvaluateSpoutProxy(InstanceStreamSource stream_src,
				IStateFactory classifierState, long pending) {
			super(stream_src, classifierState, pending);
			// TODO Auto-generated constructor stub
		}
		
		protected void setKey(TopologyContext context) {
			m_key = 1;
		}
		
	}

	@Test
	public void test() {
		DummyPersistentState dummy_state = new DummyPersistentState();
		DummyStateFactory dummy_factory = new DummyStateFactory();
		dummy_factory.the_state = dummy_state;
		RandomTreeGenerator generator = new RandomTreeGenerator();
		generator.prepareForUse();
		generator.restart();
		MOAStreamSpout spout = new MOAStreamSpout(generator,0);
		EvaluateSpoutProxy proxy = new EvaluateSpoutProxy(spout, dummy_factory, 1);
		proxy.open(null, null, null);
		
		proxy.reset();
		proxy.nextTuple();
		assertEquals(proxy.update_emitted, true);
		assertEquals(proxy.evaluation_emitted, false);

		
		proxy.reset();
		proxy.nextTuple();
		assertEquals(proxy.update_emitted, false);
		assertEquals(proxy.evaluation_emitted, false);
		
		proxy.reset();
		proxy.ack(new MessageIdentifier(-1,0));
		
		proxy.reset();
		proxy.nextTuple();
		assertEquals(proxy.update_emitted, false);
		assertEquals(proxy.evaluation_emitted, true);
		
		dummy_state.the_long = 10;
		proxy.reset();
		
		proxy.nextTuple();
		assertEquals(proxy.update_emitted, true);
		assertEquals(proxy.evaluation_emitted, true);
		
	}

}

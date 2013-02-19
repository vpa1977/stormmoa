package performance;

import static org.junit.Assert.assertEquals;
import moa.storm.persistence.IStateFactory;
import moa.storm.topology.message.MessageIdentifier;
import moa.storm.topology.meta.bolts.EvaluateSpout;
import moa.storm.topology.spout.InstanceStreamSource;
import moa.streams.generators.RandomTreeGenerator;

import org.junit.Test;

import performance.state.DummyPersistentState;
import performance.state.DummyStateFactory;
import backtype.storm.task.TopologyContext;

public class EvaluateSpoutTest {
	
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

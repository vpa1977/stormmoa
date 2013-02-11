package moa.storm.topology.meta.bolts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


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

public class EvaluateClassifierBolt extends BaseRichBolt implements IRichBolt
{
	private OutputCollector m_collector;
	private String m_key;

//	private transient IPersistentState<LearnerWrapper> m_classifier_state;
	private transient Classifier m_wrapper;
	private IStateFactory m_state_factory;
	private long m_version;
	private long m_pending;

	public EvaluateClassifierBolt(IStateFactory classifierState) {
		m_state_factory = classifierState;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
	//	m_classifier_state = ((IStateFactory) m_state_factory).create();
		m_wrapper = null;
		m_key = "classifier" + context.getThisTaskIndex();
		m_version = -1;
	}

	@Override
	public void execute(Tuple tuple) {
		
		m_version = tuple.getLongByField("version").longValue();
		
		if (m_wrapper == null) {
			//System.out.println("Try to fetch "+ m_version + " -> "+ m_key);
			m_wrapper = (Classifier) SharedStorageBolt.instance().get(m_key, m_version);
			//System.out.println("result "+ m_wrapper);
		}
		
		if (m_wrapper != null) {
			Object instance_id = tuple.getValue(0);
			Object instance = tuple.getValue(1);
			
			List<Object> objs = new ArrayList<Object>();
			objs.add(instance_id);
			objs.add( instance );
			ArrayList< DoubleVector > results = new ArrayList<DoubleVector>();
			List<Instance> list = (List<Instance>) instance;
			Iterator<Instance> it = list.iterator();
			while (it.hasNext())
				results.add(new DoubleVector(m_wrapper.getVotesForInstance( it.next() )));
				
			objs.add(results);
			objs.add( 1 );
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
		declarer.declare(new Fields("instance_id", "instance", "prediction","votes", "timestamp"));
	}
}
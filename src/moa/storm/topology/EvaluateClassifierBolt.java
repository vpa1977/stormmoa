package moa.storm.topology;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import moa.core.DoubleVector;
import moa.storm.cassandra.CassandraState;
import moa.storm.tasks.LearnerWrapper;
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

	private transient CassandraState<LearnerWrapper> m_classifier_state;
	private transient LearnerWrapper m_wrapper;
	private StateFactory m_state_factory;
	private long m_version;
	private long m_pending;

	public EvaluateClassifierBolt(StateFactory classifierState) {
		m_state_factory = classifierState;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
		m_classifier_state = ((CassandraState.Factory) m_state_factory).create();
		m_wrapper = null;
		m_key = "classifier" + context.getThisTaskIndex();
		m_version = -1;
	}

	@Override
	public void execute(Tuple tuple) {
		
		Object index_field = tuple.getValue(0);
		if (index_field instanceof EnsembleCommand) {
			if (index_field instanceof Reset) {
				Reset reset = (Reset) index_field;
				m_version = reset.version();
				System.out.println("got version field "+ m_version);
				m_pending = reset.pending();
				m_wrapper = null;
			}
		}
		else
		{
			if (m_wrapper == null) {
				System.out.println("Try to fetch "+ m_version + " -> "+ m_key);
				m_wrapper = (LearnerWrapper) m_classifier_state.get(String.valueOf(m_version), m_key);
				System.out.println("result "+ m_wrapper);
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
					results.add(new DoubleVector(m_wrapper.getClassifier().getVotesForInstance( it.next() )));
					
				objs.add(results);
				objs.add( 1 );
				objs.add(System.currentTimeMillis());
				m_collector.emit(tuple,objs);
			}
		}
		
		m_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance_id", "instance", "prediction","votes", "timestamp"));
	}
}
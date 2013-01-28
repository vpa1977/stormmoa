package moa.storm.topology;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import moa.classifiers.Classifier;
import moa.core.MiscUtils;
import moa.options.ClassOption;
import moa.options.Option;
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

/**
 * Train the specified classifier.
 * 
 * @author bsp
 * 
 */
public class TrainClassifierBolt extends BaseRichBolt implements IRichBolt {
	public static int INSTANCES = 0;
	private OutputCollector m_collector;

	private String m_cli_string;
	public Random classifierRandom = new Random();
	private String m_key;

	private transient CassandraState<LearnerWrapper> m_classifier_state;
	private transient LearnerWrapper m_wrapper;

	private StateFactory m_state_factory;
	private long m_version;
	private long m_pending;

	private Classifier getClassifier(String cliString) throws RuntimeException {
		Classifier cls;
		try {
			cls = (Classifier) ClassOption.cliStringToObject(cliString,
					Classifier.class, new Option[] {});
			cls.prepareForUse();
			cls.resetLearning();
			return cls;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public TrainClassifierBolt(String cliString, StateFactory classifierState) {
		m_cli_string = cliString;
		m_state_factory = classifierState;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;

		m_classifier_state = ((CassandraState.Factory) m_state_factory)
				.create();
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
				m_pending = reset.pending();
				m_wrapper = (LearnerWrapper) m_classifier_state.get(
						String.valueOf(m_version), m_key);
				if (m_wrapper == null) {
					m_wrapper = new LearnerWrapper(getClassifier(m_cli_string));
				}
				m_version++;

			}
		} else if (m_wrapper != null) {
			List<Instance> list = (List<Instance>) tuple.getValue(1);
			Iterator<Instance> it = list.iterator();
			while (it.hasNext()) {
				Instance value = it.next();
				int weight = MiscUtils.poisson(1.0, this.classifierRandom);
				if (weight > 0) {
					Instance trainInst = (Instance) (value).copy();
					trainInst.setWeight(trainInst.weight() * weight);
					m_wrapper.getClassifier().trainOnInstance(trainInst);
					m_wrapper.increaseInstancesProcessed();
				}
			}
			if (m_version % m_pending == 0) 
			{
				m_classifier_state.put(String.valueOf(m_version), m_key,m_wrapper);
			}
			m_version ++;
		} else
			throw new RuntimeException("Learning bolt is not initialized");
		m_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "classifier"));
	}

}
package performance.ozabag_distributed.bolts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import performance.ozabag_distributed.BaggingMember;


import moa.classifiers.Classifier;
import moa.classifiers.meta.OzaBoost;
import moa.core.MiscUtils;
import moa.options.ClassOption;
import moa.options.Option;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.IPersistentState;
import moa.storm.tasks.LearnerWrapper;
import moa.storm.topology.EnsembleCommand;
import moa.storm.topology.Reset;
import storm.trident.state.StateFactory;
import weka.core.Instance;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Trains the specified classifier.
 * 
 * @author bsp
 * 
 */
public class BaggingTrainBolt extends BasePartition implements IRichBolt {
	private OutputCollector m_collector;

	private String m_cli_string;
	public Random classifierRandom = new Random();
	private String m_key;
	private transient IPersistentState<BaggingMember> m_classifier_state;
	private transient List<BaggingMember> m_wrapper;
	private IStateFactory m_state_factory;
	private long m_version;
	private long m_pending;
	private List<String> m_fields;
	private int m_save_task;
	private long m_last_sent_version;
	private boolean m_b_first_save;
	private boolean pureBoostOption;

	private Classifier createClassifier(String cliString) throws RuntimeException {
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

	public BaggingTrainBolt(int ensemble_size,List<String> fields, String cliString, IStateFactory classifierState) {
		super(ensemble_size);
		m_cli_string = cliString;
		m_state_factory = classifierState;
		m_fields = fields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf,context,collector);
		m_collector = collector;
		m_classifier_state = ((IStateFactory) m_state_factory).create();
		m_wrapper = null;
		m_key = "classifier" + context.getThisTaskIndex();
		m_version = -1;
		m_b_first_save =false;
		
	}

	@Override
	public void execute(Tuple tuple) {
		
		Object index_field = tuple.getValue(0);
		if (index_field instanceof EnsembleCommand) {
			if (index_field instanceof Reset) {
				Reset reset = (Reset) index_field;
				m_version = reset.version();
				m_pending = reset.pending();
				m_last_sent_version = m_version;
				m_wrapper = new ArrayList<BaggingMember>();
				
				for (int i = m_start_key ; i < m_start_key+m_partition_size; i ++ )
				{
					BaggingMember member =m_classifier_state.get("classifier"+i,String.valueOf(m_version));
					if (member == null)
					{
						m_wrapper = null;
						break;
					}
					m_wrapper.add(member); 
				}
				
				if (m_wrapper == null) {
					m_b_first_save =true;
					m_wrapper = new ArrayList<BaggingMember>();
					for (int i = m_start_key ; i < m_start_key+m_partition_size; i ++ )
					{
						BaggingMember member  = new BaggingMember();
						member.m_classifier = createClassifier(m_cli_string);
						member.m_key = "classifier" +i;
						m_wrapper.add(member);
						
					}
				}
			}
		} else if (m_wrapper != null) {
			long version = tuple.getLongByField("version").longValue();
			Instance inst = (Instance)tuple.getValueByField("instance");
			Boolean persist = tuple.getBooleanByField("persist");
			if (persist.booleanValue())
				System.out.println("Should save "+version);
			for (BaggingMember wrapper : m_wrapper)
			{

				int weight = MiscUtils.poisson(1.0, this.classifierRandom);
				if (weight > 0) {
					Instance trainInst = (Instance) (inst).copy();
					trainInst.setWeight(trainInst.weight() * weight);
					wrapper.m_classifier.trainOnInstance(trainInst);
				}
			}

			if (persist.booleanValue() && CanPersist(version)) 
			{
				m_version = version;
				ArrayList<Object> message = new ArrayList<Object>();
				message.add(m_version);
				ArrayList<BaggingMember> copy = new ArrayList<BaggingMember>();
				for (BaggingMember m : m_wrapper)
				{
					BaggingMember b_copy = new BaggingMember(m);
					copy.add(b_copy);
				}
				message.add(copy);
				m_collector.emit("persist",  message);
			}
		} else
			throw new RuntimeException("Learning bolt is not initialized");
		m_collector.ack(tuple);
	}

	private boolean CanPersist(long version) {
		if (m_version < version || m_b_first_save) 
		{
			long saved_version = m_classifier_state.getLong("version", "version");
			if (saved_version >= m_version ||m_b_first_save)
			{
				System.out.println("Saving from "+m_start_key + " version "+ version + " saved version "+saved_version);
				m_b_first_save =false;
				return true;
			}
		}
		return false;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("learn", new Fields(m_fields));
		declarer.declareStream("persist",new Fields("version","ensemble_partition"));
		
	}

}
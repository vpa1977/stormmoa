package moa.storm.topology.meta.bolts.partitioned.ozaboost;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import moa.classifiers.Classifier;
import moa.core.MiscUtils;
import moa.options.ClassOption;
import moa.options.Option;
import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.ensemble_members.BoostingMember;
import moa.storm.topology.message.EnsembleCommand;
import moa.storm.topology.message.Reset;
import moa.storm.topology.meta.bolts.partitioned.BasePartition;
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
public class BoostTrainBolt extends BasePartition implements IRichBolt {
	private OutputCollector m_collector;

	private String m_cli_string;
	public Random classifierRandom = new Random();
	private String m_key;
	private transient IPersistentState<BoostingMember> m_classifier_state;
	private transient List<BoostingMember> m_wrapper;
	private IStateFactory m_state_factory;
	private long m_version;
	private long m_pending;
	private List<String> m_fields;
	private int m_save_task;
	private long m_last_sent_version;
	private boolean m_b_first_save = false;
	private boolean pureBoostOption;
	private boolean m_last;

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

	public BoostTrainBolt(int ensemble_size,List<String> fields, String cliString, IStateFactory classifierState) {
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
				m_wrapper = new ArrayList<BoostingMember>();
				
				for (int i = m_start_key ; i < m_start_key+m_partition_size; i ++ )
				{
					BoostingMember member =m_classifier_state.get("classifier"+i,String.valueOf(m_version));
					if (member == null)
					{
						m_wrapper = null;
						break;
					}
					m_wrapper.add(member); 
				}
				
				if (m_wrapper == null) {
					m_b_first_save =true;
					m_wrapper = new ArrayList<BoostingMember>();
					for (int i = m_start_key ; i < m_start_key+m_partition_size; i ++ )
					{
						BoostingMember member  = new BoostingMember();
						member.m_classifier = createClassifier(m_cli_string);
						member.m_key = "classifier" +i;
						m_wrapper.add(member);
						
					}
				}
			}
		} else if (m_wrapper != null) {
			double lambda_d = tuple.getDoubleByField("lambda_d").doubleValue();
			long version = tuple.getLongByField("version").longValue();
			Instance inst = (Instance)tuple.getValueByField("instance");
			Boolean persist = tuple.getBooleanByField("persist");
			
			for (BoostingMember wrapper : m_wrapper)
			{
				double k = pureBoostOption ? lambda_d : MiscUtils.poisson(lambda_d, this.classifierRandom);
				wrapper.m_training_weight_seen_by_model += inst.weight();
	            if (k > 0.0) {
	                Instance weightedInst = (Instance) inst.copy();
	                weightedInst.setWeight(inst.weight() * k);
	                wrapper.m_classifier.trainOnInstance(weightedInst);
	            }
	            if (wrapper.m_classifier.correctlyClassifies(inst)) {
	            	wrapper.m_scms += lambda_d;
	                lambda_d *= wrapper.m_training_weight_seen_by_model / (2 * wrapper.m_scms);
	            } else {
	            	wrapper.m_swms += lambda_d;
	                lambda_d *= wrapper.m_training_weight_seen_by_model / (2 * wrapper.m_swms);
	            }
			}

			if (persist.booleanValue() && CanPersist(version)) 
			{
				m_version = version;
				ArrayList<Object> message = new ArrayList<Object>();
				message.add(m_version);
				ArrayList<BoostingMember> copy = new ArrayList<BoostingMember>();
				for (BoostingMember m : m_wrapper)
				{
					BoostingMember b_copy = new BoostingMember(m);
					copy.add(b_copy);
				}
				message.add(copy);
				m_collector.emit("persist",  message);
			}
			
			if (!m_blast)
			{
				ArrayList output =  new ArrayList(tuple.getValues());
				output.set( tuple.getFields().fieldIndex("lambda_d"), lambda_d);
				int index = tuple.getIntegerByField("index").intValue();
				output.set( tuple.getFields().fieldIndex("index"), ++index);
				m_collector.emit(tuple.getSourceStreamId(), tuple, output);
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
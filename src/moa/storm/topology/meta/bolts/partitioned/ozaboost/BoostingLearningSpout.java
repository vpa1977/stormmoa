package moa.storm.topology.meta.bolts.partitioned.ozaboost;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import moa.storm.topology.message.EnsembleCommand;
import moa.storm.topology.message.MessageIdentifier;
import moa.storm.topology.message.Reset;
import moa.storm.topology.spout.InstanceStreamSource;
import weka.core.Instance;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class BoostingLearningSpout extends BaseRichSpout implements IRichSpout {

	public static final List<String> LEARN_STREAM_FIELDS = Arrays.asList( new String[]{"instance", "lambda_d","version","persist"});
	public static final String NOTIFICATION_STREAM = "notification";
	public static final String COMMAND_FIELD = "command";
	private static final int NOTIFICATION_ID = -1;
	public static final String EVENT_STREAM = "events";

	private InstanceStreamSource m_stream_src;
	private IStateFactory m_classifier_state;
	private SpoutOutputCollector m_collector;
	private IPersistentState<String> m_state;
	private long m_version;
	private boolean m_reset;
	
	private long m_id;
	private long m_pending;
	private int m_key;

	
	private ArrayList<Instance> m_instance_cache;
	private long m_sent_to_save;
	

	public BoostingLearningSpout(InstanceStreamSource stream_src, IStateFactory classifierState,
			long pending) {
		m_stream_src = stream_src;
		m_classifier_state = classifierState;
		m_pending = pending;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		m_collector = collector;
		
		m_state = ((IStateFactory) m_classifier_state).create();
		m_version = readVersion(m_state)+1;
		if (m_version <0 )
			m_version =0;
		m_sent_to_save = -1;
		m_reset = true;
	
		m_id = 0;
		m_key = context.getThisTaskId();
		
	}


	@Override
	public void nextTuple() {
		if (m_reset) {
			m_collector.emit(
					NOTIFICATION_STREAM,
					new ArrayList<Object>(Arrays
							.asList(new EnsembleCommand[] { new Reset(
									m_version, m_pending) })),
					new MessageIdentifier(NOTIFICATION_ID, m_id++));
			m_reset = false;
		}
		if (m_instance_cache == null || m_instance_cache.size() == 0)
			m_instance_cache = m_stream_src.read();
		
		List<Object> message = new ArrayList<Object>();
		message.add(m_instance_cache.remove(0));
		message.add(1.0);
		message.add(m_version);
		if (m_version % m_pending == 0 && readVersion(m_state) >= m_sent_to_save)
		{
			message.add(true);
			m_sent_to_save = m_version;
		}
		else
			message.add(false);
		
			
		m_collector.emit(EVENT_STREAM, message, new MessageIdentifier(m_key,m_version));

	}

	@Override
	public void fail(Object msgId) {
		if (m_sent_to_save > 0)
		{
			m_reset = true;
			m_sent_to_save =-1;
		}
		super.fail(msgId);
	}

	@Override
	public void ack(Object msgId) {
		m_version++;		
		super.ack(msgId);


	}

	private long readVersion(IPersistentState state) {
		return state.getLong("version", "version");
	}



	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(EVENT_STREAM,new Fields(LEARN_STREAM_FIELDS));
		declarer.declareStream(NOTIFICATION_STREAM, new Fields(COMMAND_FIELD));
	}
	
	
	
	
	
}

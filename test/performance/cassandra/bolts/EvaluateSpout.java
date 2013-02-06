package performance.cassandra.bolts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import performance.cassandra.InstanceStreamSource;

import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.IPersistentState;
import moa.storm.topology.EnsembleCommand;
import moa.storm.topology.MessageIdentifier;
import moa.storm.topology.Reset;
import storm.trident.state.StateFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class EvaluateSpout extends BaseRichSpout implements IRichSpout {
	
	public static final List<String> EVALUATE_STREAM_FIELDS = Arrays.asList( new String[]{"instance", "version"});
	public static final String NOTIFICATION_STREAM = "notification";
	public static final String COMMAND_FIELD = "command";
	private static final int NOTIFICATION_ID = -1;

	private InstanceStreamSource m_stream_src;
	private IStateFactory m_classifier_state;
	private SpoutOutputCollector m_collector;
	private IPersistentState<String> m_state;
	private long m_version;
	private long m_current_version;
	private long m_id;
	private boolean m_reset;
	private boolean m_update_pending;
	private long m_pending;
	private static int instance_count = 0;
	protected int m_key;
	private long m_clean_start;

	public EvaluateSpout(InstanceStreamSource stream_src, IStateFactory classifierState,
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
		m_version = readVersion(m_state);
		if (m_version < 0)
			m_version = 0;
		
		m_current_version = -1;
		m_reset = false;
		m_id = 0;
		m_clean_start = 0;
		setKey(context);
		instance_count ++ ;
		if (instance_count > 1) 
			throw new RuntimeException("This should have only 1 instance");

	}

	protected void setKey(TopologyContext context) {
		m_key = context.getThisTaskId();
	}


	@Override
	public void nextTuple() {
		
		if (!m_update_pending)
		{
			if (m_id % m_pending == 0)
			{
				
				m_version = readVersion(m_state);
				if (m_version < 0)
					m_version = -1;
				if (m_version != m_current_version)
				{
					m_reset = true;
				}
				
			}
			
			if (m_reset)
			{
				m_reset = false;
				m_update_pending = true;
				emitUpdateMessage();
			}
		}
		
		
		if (m_current_version >=0)
		{
			emitEvaluationMessage();
		}

	}

	protected void emitUpdateMessage() {
		m_collector.emit(
				NOTIFICATION_STREAM,
				new ArrayList<Object>(Arrays
						.asList(new EnsembleCommand[] { new Reset(
								m_version, m_pending) })),
				new MessageIdentifier(NOTIFICATION_ID, m_version));
	}

	protected void emitEvaluationMessage() {
		List<Object> message = new ArrayList<Object>();
		message.add(m_stream_src.read());
		message.add(m_current_version);		
		m_collector.emit(message, new MessageIdentifier(m_key,m_id++));
	}

	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		MessageIdentifier id = (MessageIdentifier) msgId;
		if (id.getTask()  == NOTIFICATION_ID)
		{
			
			m_reset = true;
			m_update_pending = false;
		}
	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
		MessageIdentifier id = (MessageIdentifier) msgId;
		if (id.getTask()  == NOTIFICATION_ID) 
		{
			m_current_version = id.getId();
			m_state.setLong("version", "evaluate_version", m_current_version);
			m_update_pending = false;
		}
	}
	


	private long readVersion(IPersistentState state) {
		return state.getLong("version", "version");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(EVALUATE_STREAM_FIELDS));
		declarer.declareStream(NOTIFICATION_STREAM, new Fields(COMMAND_FIELD));
	}


}

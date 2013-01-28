package moa.storm.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import moa.storm.cassandra.CassandraState;
import storm.trident.state.StateFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class EvaluateSpout extends BaseRichSpout implements IRichSpout {

	public static final String NOTIFICATION_STREAM = "notification";
	public static final String COMMAND_FIELD = "command";

	private IRichSpout m_stream_src;
	private StateFactory m_classifier_state;
	private SpoutOutputCollector m_collector;
	private CassandraState<String> m_state;
	private long m_version;
	private boolean m_reset;
	private int m_task_id;
	private long m_id;
	private long m_pending;

	public EvaluateSpout(IRichSpout stream_src, StateFactory classifierState,
			long pending) {
		m_stream_src = stream_src;
		m_classifier_state = classifierState;
		m_pending = pending;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		m_collector = collector;
		m_stream_src.open(conf, context, collector);
		m_state = ((CassandraState.Factory) m_classifier_state).create();
		m_version = readVersion(m_state);
		m_reset = true;
		m_task_id = context.getThisTaskId();
		m_id = 0;

	}

	private long readVersion(CassandraState state) {
		return state.getLong("version", "version");
	}

	@Override
	public void nextTuple() {
		// check periodically for the updated classifier version
		if (m_id % m_pending == 0)
		{
			long version = readVersion(m_state);
			if (version != m_version)
			{
				m_version = version;
				m_reset  = true;
			}
		}
		
		if (m_reset) {
			m_collector.emit(
					NOTIFICATION_STREAM,
					new ArrayList<Object>(Arrays
							.asList(new EnsembleCommand[] { new Reset(
									m_version, m_pending) })),
					new MessageIdentifier(m_task_id, m_id++));
			m_reset = false;
		}
		m_stream_src.nextTuple();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		m_stream_src.declareOutputFields(declarer);
		declarer.declareStream(NOTIFICATION_STREAM, new Fields(COMMAND_FIELD));
	}

	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		m_reset = true;
	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
	}

	private void updateVersion(CassandraState state, long l) {
		state.setLong("version", "version", l);
		state.cleanup(String.valueOf(l - 1));
	}

}

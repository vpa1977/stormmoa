package performance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import moa.storm.topology.message.MessageIdentifier;
import moa.storm.topology.spout.InstanceStreamSource;
import moa.streams.InstanceStream;
import weka.core.Instance;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class MOAStreamSpout extends BaseRichSpout implements IRichSpout, InstanceStreamSource {
	
	private static final long serialVersionUID = 2296373075956453004L;
	private long m_acked;
	private SpoutOutputCollector m_collector;
	private long m_delay;
	private long m_id;
	private int m_key;
	private Measurement m_measurement;
	private long m_sent;
	
	private InstanceStream m_stream;

	public MOAStreamSpout(InstanceStream stream, int delay)
	{
		m_stream = stream;
		m_delay = delay;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance"));
	}
	
	@Override
	public void nextTuple() {
			if (m_measurement == null)
				m_measurement = new Measurement();
			m_measurement.check();
			if (m_delay == 0 || m_id < m_delay ) 
			{
				List<Object> message = new ArrayList<Object>();
				message.add(read());
				m_collector.emit(message, new MessageIdentifier(m_key,m_id++));
				if (m_id % 10000 == 0 && m_delay == 0)
				{
					System.out.println("Emitted "+m_id+" tuples");
				}
				
			}
		
	}


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		m_collector = collector;
		m_stream.restart();
		m_id = 0;
		m_acked= 0;
		m_key = context.getThisTaskId();
	}
	
	@Override
	public ArrayList<Instance> read() {
		ArrayList<Instance> instances = new ArrayList<Instance>();
		for (int i = 0 ; i < 100 ; i++)
			instances.add(m_stream.nextInstance());
		return instances;
	}


}

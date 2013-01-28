package moa.storm.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import moa.streams.InstanceStream;
import weka.core.Instance;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class MOAStreamSpout extends BaseRichSpout implements IRichSpout{
	

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}

	private InstanceStream m_stream;
	private SpoutOutputCollector m_collector;
	private long m_id;
	private int m_key;
	private long m_delay;
	private long m_sent;
	private long m_acked;
	
	public MOAStreamSpout(InstanceStream stream, int delay)
	{
		m_stream = stream;
		m_delay = delay;
	}

	private static final long serialVersionUID = 2296373075956453004L;

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
	public void nextTuple() {
			if (m_delay == 0 || m_id < m_delay ) 
			{
				List<Object> message = new ArrayList<Object>();
				ArrayList<Instance> instances = new ArrayList<Instance>();
				for (int i = 0 ; i < 100 ; i++)
					instances.add(m_stream.nextInstance());
				message.add(instances);
				m_collector.emit(message, new MessageIdentifier(m_key,m_id++));
				if (m_id % 10000 == 0 && m_delay == 0)
				{
					System.out.println("Emitted "+m_id+" tuples");
				}
				
			}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance"));
	}

}

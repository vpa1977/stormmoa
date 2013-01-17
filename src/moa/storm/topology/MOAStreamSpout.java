package moa.storm.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import moa.streams.InstanceStream;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class MOAStreamSpout extends BaseRichSpout implements IRichSpout{
	
	private InstanceStream m_stream;
	private SpoutOutputCollector m_collector;
	private long m_id;
	private long m_delay;

	
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
	}

	@Override
	public void nextTuple() {
		List<Object> message = new ArrayList<Object>();
		message.add(m_stream.nextInstance());
		message.add(m_id++);
		m_collector.emit(message);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance", "tuple_id"));
	}

}

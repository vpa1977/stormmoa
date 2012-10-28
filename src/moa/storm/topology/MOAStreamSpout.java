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

public class MOAStreamSpout extends BaseRichSpout implements IRichSpout{
	
	private InstanceStream m_stream;
	private Scheme m_scheme;
	private SpoutOutputCollector m_collector;
	
	public MOAStreamSpout( Scheme scheme, InstanceStream stream)
	{
		m_scheme = scheme;
		m_stream = stream;
	}

	private static final long serialVersionUID = 2296373075956453004L;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		m_collector = collector;
		m_stream.restart();
	}

	@Override
	public void nextTuple() {
		List<Object> message = new ArrayList<Object>();
		message.add(m_stream.nextInstance());
		m_collector.emit(message);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(m_scheme.getOutputFields());
	}

}

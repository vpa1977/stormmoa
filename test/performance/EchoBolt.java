package performance;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class EchoBolt extends BaseRichBolt implements IRichBolt {

	private OutputCollector m_collector;
	private String m_stream_id;
	
	public EchoBolt(String streamId)
	{
		m_stream_id = streamId;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		m_collector.emit(m_stream_id,tuple,tuple.getValues());
		m_collector.ack(tuple);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(m_stream_id,new Fields("instance_id", "instance"));
	}


}

package performance.ozaboost_distributed.bolts;

import java.util.Map;



import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import moa.storm.topology.MessageIdentifier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class VersionUpdateBolt extends BaseRichBolt implements IRichBolt {

	private IStateFactory m_state_factory;
	private IPersistentState m_state;
	private int m_num_tasks;
	private int m_cur_count;
	private long m_version;
	
	public VersionUpdateBolt(IStateFactory fact, int num_tasks)
	{
		m_num_tasks= num_tasks;
		m_state_factory = fact;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_state = m_state_factory.create();
		
	}

	@Override
	public void execute(Tuple input) {
		
		long version = input.getLongByField("version");
		if (version != m_version)
		{
			m_version = version;
			m_cur_count = 1;
		}
		else {
			m_cur_count++;
			if (m_cur_count == m_num_tasks) 
			{
				updateVersion(m_state,m_version);
				m_cur_count = 0;
			}
		}
		
	}
	

	
	private void updateVersion(IPersistentState state, long l) {
		state.setLong("version", "version", l);
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
